const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const SRC_FILES = [
  'Config.js',
  'OdpsSigner.js',
  'SettingsParser.js',
  'SqlExecutor.js',
  'TableBrowser.js',
  'Code.js',
  'Test.js'
];

class XmlAttribute {
  constructor(value) {
    this.value = value;
  }

  getValue() {
    return this.value;
  }
}

class XmlElement {
  constructor(name, attrs) {
    this.name = name;
    this.attrs = attrs || {};
    this.children = [];
    this.textParts = [];
  }

  getName() {
    return this.name;
  }

  getChild(name) {
    return this.children.find((child) => child.name === name) || null;
  }

  getChildren(name) {
    if (!name) return this.children.slice();
    return this.children.filter((child) => child.name === name);
  }

  getText() {
    return this.textParts.join('') + this.children.map((child) => child.getText()).join('');
  }

  getAttribute(name) {
    if (!Object.prototype.hasOwnProperty.call(this.attrs, name)) return null;
    return new XmlAttribute(this.attrs[name]);
  }
}

function decodeXml(value) {
  return String(value)
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}

function parseXmlAttrs(raw) {
  const attrs = {};
  const attrRe = /([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g;
  let match;
  while ((match = attrRe.exec(raw))) {
    attrs[match[1]] = decodeXml(match[2] ?? match[3] ?? '');
  }
  return attrs;
}

function parseXml(xml) {
  const clean = String(xml)
    .replace(/<\?xml[\s\S]*?\?>/g, '')
    .replace(/<!--[\s\S]*?-->/g, '');
  const tokenRe = /<!\[CDATA\[([\s\S]*?)\]\]>|<[^>]+>|[^<]+/g;
  const stack = [];
  let root = null;
  let match;

  while ((match = tokenRe.exec(clean))) {
    const token = match[0];
    if (!token) continue;

    if (token.startsWith('<![CDATA[')) {
      if (stack.length) stack[stack.length - 1].textParts.push(match[1] || '');
      continue;
    }

    if (token[0] !== '<') {
      if (stack.length) stack[stack.length - 1].textParts.push(decodeXml(token));
      continue;
    }

    if (token.startsWith('</')) {
      stack.pop();
      continue;
    }

    if (token.startsWith('<!') || token.startsWith('<?')) {
      continue;
    }

    const selfClosing = token.endsWith('/>');
    const body = token.slice(1, selfClosing ? -2 : -1).trim();
    const name = body.split(/\s+/, 1)[0];
    const elem = new XmlElement(name, parseXmlAttrs(body));

    if (stack.length) {
      stack[stack.length - 1].children.push(elem);
    } else {
      root = elem;
    }

    if (!selfClosing) stack.push(elem);
  }

  if (!root) throw new Error('Invalid XML: missing root element');
  return {
    getRootElement() {
      return root;
    }
  };
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;
  const input = String(text || '');

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (ch === '"') {
      if (inQuotes && input[i + 1] === '"') {
        cell += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      row.push(cell);
      cell = '';
    } else if (ch === '\n' && !inQuotes) {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = '';
    } else if (ch !== '\r') {
      cell += ch;
    }
  }

  if (cell.length || row.length || input.endsWith(',')) {
    row.push(cell);
    rows.push(row);
  }

  return rows;
}

function makeUtilities() {
  return {
    MacAlgorithm: { HMAC_SHA_1: 'HMAC_SHA_1' },
    DigestAlgorithm: { MD5: 'MD5' },
    Charset: { UTF_8: 'UTF_8' },

    computeHmacSignature(algorithm, value, secret) {
      if (algorithm !== 'HMAC_SHA_1') throw new Error(`Unsupported HMAC algorithm: ${algorithm}`);
      return Array.from(crypto.createHmac('sha1', secret).update(String(value), 'utf8').digest());
    },

    computeDigest(algorithm, value) {
      if (algorithm !== 'MD5') throw new Error(`Unsupported digest algorithm: ${algorithm}`);
      return Array.from(crypto.createHash('md5').update(String(value), 'utf8').digest());
    },

    base64Encode(bytes) {
      return Buffer.from(bytes).toString('base64');
    },

    base64Decode(value) {
      return Array.from(Buffer.from(String(value), 'base64'));
    },

    newBlob(bytes) {
      return {
        getDataAsString() {
          return Buffer.from(bytes).toString('utf8');
        }
      };
    },

    parseCsv,

    formatDate(date, timezone, pattern) {
      const d = new Date(date);
      if (pattern === "EEE, dd MMM yyyy HH:mm:ss 'GMT'") {
        return d.toUTCString().replace('UTC', 'GMT');
      }
      if (pattern === 'yyyyMMddHHmmss') {
        const pad = (n) => String(n).padStart(2, '0');
        return [
          d.getUTCFullYear(),
          pad(d.getUTCMonth() + 1),
          pad(d.getUTCDate()),
          pad(d.getUTCHours()),
          pad(d.getUTCMinutes()),
          pad(d.getUTCSeconds())
        ].join('');
      }
      return d.toISOString();
    },

    getUuid() {
      return '11111111-2222-3333-4444-555555555555';
    },

    sleep() {}
  };
}

function createPropertiesStore(initial) {
  const values = { ...(initial || {}) };
  return {
    getProperties() {
      return { ...values };
    },
    getProperty(key) {
      return values[key] || null;
    },
    setProperty(key, value) {
      values[key] = String(value);
    },
    deleteProperty(key) {
      delete values[key];
    },
    _values: values
  };
}

class MockRange {
  constructor(sheet, row, col, numRows, numCols) {
    this.sheet = sheet;
    this.row = row;
    this.col = col;
    this.numRows = numRows;
    this.numCols = numCols;
  }

  setValues(values) {
    if (values.length !== this.numRows) {
      throw new Error(`setValues row count mismatch: expected ${this.numRows}, got ${values.length}`);
    }
    for (const row of values) {
      if (!Array.isArray(row) || row.length !== this.numCols) {
        throw new Error(`setValues column count mismatch: expected ${this.numCols}`);
      }
    }
    this.sheet.setValues(this.row, this.col, values);
    return this;
  }

  setFontWeight(value) {
    this.sheet.calls.push(['setFontWeight', this.row, this.col, value]);
    return this;
  }

  setBackground(value) {
    this.sheet.calls.push(['setBackground', this.row, this.col, value]);
    return this;
  }

  setFontColor(value) {
    this.sheet.calls.push(['setFontColor', this.row, this.col, value]);
    return this;
  }

  setHorizontalAlignment(value) {
    this.sheet.calls.push(['setHorizontalAlignment', this.row, this.col, value]);
    return this;
  }

  clearFormat() {
    this.sheet.calls.push(['clearFormat', this.row, this.col, this.numRows, this.numCols]);
    return this;
  }
}

class MockSheet {
  constructor(name) {
    this.name = name;
    this.values = [];
    this.calls = [];
    this.maxRows = 1000;
    this.maxCols = 26;
  }

  getName() {
    return this.name;
  }

  clear() {
    this.values = [];
    this.calls.push(['clear']);
  }

  getRange(row, col, numRows, numCols) {
    return new MockRange(this, row, col, numRows, numCols);
  }

  getMaxRows() {
    return this.maxRows;
  }

  getMaxColumns() {
    return this.maxCols;
  }

  setValues(row, col, values) {
    for (let r = 0; r < values.length; r++) {
      const targetRow = row - 1 + r;
      if (!this.values[targetRow]) this.values[targetRow] = [];
      for (let c = 0; c < values[r].length; c++) {
        this.values[targetRow][col - 1 + c] = values[r][c];
      }
    }
    this.calls.push(['setValues', row, col, values]);
  }

  setFrozenRows(count) {
    this.calls.push(['setFrozenRows', count]);
  }

  autoResizeColumn(col) {
    this.calls.push(['autoResizeColumn', col]);
  }

  activate() {
    this.calls.push(['activate']);
  }
}

class MockSpreadsheet {
  constructor(options) {
    this.name = options?.name || 'QA Workbook';
    this.id = options?.id || 'spreadsheet-123';
    this.sheets = new Map();
    this.activeSheet = this.insertSheet('Sheet1');
  }

  getName() {
    return this.name;
  }

  getId() {
    return this.id;
  }

  getSheetByName(name) {
    return this.sheets.get(name) || null;
  }

  insertSheet(name) {
    if (!name || /[\[\]\*\?\/\\:]/.test(name) || name.length > 100) {
      throw new Error(`Invalid sheet name: ${name}`);
    }
    const sheet = new MockSheet(name);
    this.sheets.set(name, sheet);
    return sheet;
  }

  getSheets() {
    return Array.from(this.sheets.values());
  }

  getActiveSheet() {
    return this.activeSheet;
  }

  toast() {}
}

function makeHttpResponse(code, text, headers) {
  return {
    getResponseCode() {
      return code;
    },
    getContentText() {
      return text || '';
    },
    getHeaders() {
      return headers || {};
    }
  };
}

function createMockMenu(label, type, calls) {
  return {
    _label: label,
    _type: type,

    addItem(caption, functionName) {
      calls.push(['addItem', type, label, caption, functionName]);
      return this;
    },

    addSeparator() {
      calls.push(['addSeparator', type, label]);
      return this;
    },

    addSubMenu(menu) {
      calls.push(['addSubMenu', type, label, menu && menu._label]);
      return this;
    },

    addToUi() {
      calls.push(['addToUi', type, label]);
      return this;
    }
  };
}

function loadGasContext(options = {}) {
  const repoRoot = path.resolve(__dirname, '..', '..');
  const defaultScriptProperties = {
    ALIYUN_ACCESS_KEY_ID: 'ak',
    ALIYUN_ACCESS_KEY_SECRET: 'sk',
    MC_PROJECT: 'proj',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  };
  const scriptProperties = createPropertiesStore(
    options.scriptProperties === null
      ? {}
      : { ...defaultScriptProperties, ...(options.scriptProperties || {}) }
  );
  const userProperties = createPropertiesStore(options.userProperties || {});
  const spreadsheet = options.spreadsheet || new MockSpreadsheet(options.spreadsheetOptions);
  const urlFetchCalls = [];
  const urlFetchQueue = [];
  const lockCalls = [];
  const uiCalls = [];
  const htmlTemplates = [];
  const logs = [];
  const lock = {
    tryLock(timeoutMs) {
      lockCalls.push(['tryLock', timeoutMs]);
      return options.lockAvailable !== false;
    },
    releaseLock() {
      lockCalls.push(['releaseLock']);
    }
  };

  const context = {
    console,
    Buffer,
    Logger: {
      log(value) {
        logs.push(String(value));
      }
    },
    Utilities: makeUtilities(),
    XmlService: { parse: parseXml },
    PropertiesService: {
      getScriptProperties() {
        return scriptProperties;
      },
      getUserProperties() {
        return userProperties;
      }
    },
    SpreadsheetApp: {
      getActiveSpreadsheet() {
        return spreadsheet;
      },
      getUi() {
        const ui = {
          Button: {
            OK: 'OK',
            CANCEL: 'CANCEL'
          },
          ButtonSet: {
            OK_CANCEL: 'OK_CANCEL'
          },
          createMenu(label) {
            uiCalls.push(['createMenu', label]);
            return createMockMenu(label, 'menu', uiCalls);
          },
          createAddonMenu() {
            uiCalls.push(['createAddonMenu']);
            return createMockMenu('addon', 'addon', uiCalls);
          },
          showSidebar(html) {
            uiCalls.push(['showSidebar', html]);
          },
          alert(title, message, buttonSet) {
            uiCalls.push(['alert', title, message, buttonSet]);
            return options.alertResponse || ui.Button.OK;
          },
        };
        return ui;
      }
    },
    LockService: {
      getDocumentLock() {
        lockCalls.push(['getDocumentLock']);
        return lock;
      }
    },
    HtmlService: {
      createHtmlOutputFromFile() {
        return {
          setTitle() { return this; },
          setWidth() { return this; }
        };
      },
      createTemplateFromFile() {
        const template = {
          initialData: '',
          evaluate() {
            return {
              setTitle() { return this; },
              setWidth() { return this; }
            };
          }
        };
        htmlTemplates.push(template);
        return template;
      }
    },
    Session: {
      getActiveUser() {
        return {
          getEmail() {
            return options.activeUserEmail === undefined ? 'runner@example.com' : options.activeUserEmail;
          }
        };
      },
      getTemporaryActiveUserKey() {
        return options.temporaryActiveUserKey === undefined ? 'tmp-user-key' : options.temporaryActiveUserKey;
      }
    },
    ScriptApp: {
      getOAuthToken() {
        return options.oauthToken === undefined ? 'oauth-token' : options.oauthToken;
      }
    },
    UrlFetchApp: {
      fetch(url, fetchOptions) {
        urlFetchCalls.push({ url, options: fetchOptions });
        if (!urlFetchQueue.length) {
          throw new Error(`No mock UrlFetchApp response queued for ${url}`);
        }
        const response = urlFetchQueue.shift();
        if (response instanceof Error) {
          throw response;
        }
        return response;
      }
    },
    __scriptProperties: scriptProperties,
    __userProperties: userProperties,
    __spreadsheet: spreadsheet,
    __urlFetchCalls: urlFetchCalls,
    __urlFetchQueue: urlFetchQueue,
    __lockCalls: lockCalls,
    __uiCalls: uiCalls,
    __htmlTemplates: htmlTemplates,
    __logs: logs
  };
  context.global = context;
  context.globalThis = context;

  vm.createContext(context);
  for (const file of SRC_FILES) {
    const source = fs.readFileSync(path.join(repoRoot, 'src', file), 'utf8');
    vm.runInContext(source, context, { filename: file });
  }

  return context;
}

module.exports = {
  loadGasContext,
  makeHttpResponse,
  MockSpreadsheet
};
