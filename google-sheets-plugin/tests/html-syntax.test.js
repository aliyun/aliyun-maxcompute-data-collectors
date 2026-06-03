const fs = require('node:fs');
const assert = require('node:assert/strict');
const test = require('node:test');

const { ALLOWED_PUBLIC_FUNCTIONS } = require('../scripts/verify-release-package');

const HTML_FILES = [
  'src/Sidebar.html',
  'src/Settings.html'
];

function extractScripts(html) {
  return [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)].map((match) => match[1]);
}

function normalizeAppsScriptTemplates(source) {
  return source.replace(/<\?!=\s*initialData\s*\?>/g, '{}');
}

function getSidebarScript() {
  const html = fs.readFileSync('src/Sidebar.html', 'utf8');
  const scripts = extractScripts(html);
  assert.equal(scripts.length > 0, true);
  return normalizeAppsScriptTemplates(scripts.join('\n'));
}

function getSettingsScript() {
  const html = fs.readFileSync('src/Settings.html', 'utf8');
  const scripts = extractScripts(html);
  assert.equal(scripts.length > 0, true);
  return normalizeAppsScriptTemplates(scripts.join('\n'));
}

function extractFunction(source, name) {
  const start = source.indexOf(`function ${name}(`);
  assert.notEqual(start, -1, `${name} function is missing`);

  const bodyStart = source.indexOf('{', start);
  assert.notEqual(bodyStart, -1, `${name} function body is missing`);

  let depth = 0;
  for (let i = bodyStart; i < source.length; i++) {
    if (source[i] === '{') depth++;
    if (source[i] === '}') depth--;
    if (depth === 0) {
      return source.slice(start, i + 1);
    }
  }

  throw new Error(`${name} function body is incomplete`);
}

function getTagById(html, id) {
  const match = html.match(new RegExp(`<[^>]+id="${id}"[^>]*>`));
  assert.ok(match, `${id} tag is missing`);
  return match[0];
}

function stripHtmlComments(source) {
  return source.replace(/<!--[\s\S]*?-->/g, '');
}

function extractGoogleScriptRunCallNames(source) {
  const calls = [];
  let offset = 0;
  while (true) {
    const start = source.indexOf('google.script.run', offset);
    if (start === -1) break;
    const methods = extractRunChainMethodNames(source, start);
    if (!methods.includes('withSuccessHandler') && !methods.includes('withFailureHandler')) {
      offset = start + 'google.script.run'.length;
      continue;
    }
    const serverMethods = methods.filter((name) =>
      name !== 'withSuccessHandler' && name !== 'withFailureHandler'
    );
    if (serverMethods.length) {
      calls.push(serverMethods[serverMethods.length - 1]);
    }
    offset = start + 'google.script.run'.length;
  }
  return calls;
}

function extractRunChainMethodNames(source, start) {
  const names = [];
  let depth = 0;
  let quote = '';
  let lineComment = false;
  let blockComment = false;

  for (let i = start + 'google.script.run'.length; i < source.length; i++) {
    const ch = source[i];
    const next = source[i + 1] || '';

    if (lineComment) {
      if (ch === '\n' || ch === '\r') lineComment = false;
      continue;
    }
    if (blockComment) {
      if (ch === '*' && next === '/') {
        blockComment = false;
        i++;
      }
      continue;
    }
    if (quote) {
      if (ch === '\\') {
        i++;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }

    if (ch === '/' && next === '/') {
      lineComment = true;
      i++;
      continue;
    }
    if (ch === '/' && next === '*') {
      blockComment = true;
      i++;
      continue;
    }
    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      continue;
    }

    if (ch === ';' && depth === 0) {
      break;
    }

    if (ch === '.' && depth === 0) {
      const match = source.slice(i + 1).match(/^([A-Za-z][A-Za-z0-9_]*)\s*\(/);
      if (match) names.push(match[1]);
    }

    if (ch === '(' || ch === '{' || ch === '[') depth++;
    if (ch === ')' || ch === '}' || ch === ']') depth--;
  }

  return names;
}

function maskCommentsAndStrings(source) {
  const out = Array.from(source);
  let quote = '';
  let lineComment = false;
  let blockComment = false;

  for (let i = 0; i < source.length; i++) {
    const ch = source[i];
    const next = source[i + 1] || '';

    if (lineComment) {
      if (ch === '\n' || ch === '\r') {
        lineComment = false;
      } else {
        out[i] = ' ';
      }
      continue;
    }
    if (blockComment) {
      out[i] = ' ';
      if (ch === '*' && next === '/') {
        out[i + 1] = ' ';
        blockComment = false;
        i++;
      }
      continue;
    }
    if (quote) {
      out[i] = ' ';
      if (ch === '\\') {
        out[i + 1] = ' ';
        i++;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }

    if (ch === '/' && next === '/') {
      out[i] = ' ';
      out[i + 1] = ' ';
      lineComment = true;
      i++;
      continue;
    }
    if (ch === '/' && next === '*') {
      out[i] = ' ';
      out[i + 1] = ' ';
      blockComment = true;
      i++;
      continue;
    }
    if (ch === '\'' || ch === '"' || ch === '`') {
      out[i] = ' ';
      quote = ch;
    }
  }

  return out.join('');
}

for (const file of HTML_FILES) {
  test(`${file} browser script parses`, () => {
    const html = fs.readFileSync(file, 'utf8');
    const scripts = extractScripts(html);
    if (scripts.length === 0) {
      throw new Error(`${file} does not contain a <script> block`);
    }

    for (let i = 0; i < scripts.length; i++) {
      const source = normalizeAppsScriptTemplates(scripts[i]);
      new Function(source);
    }
  });
}

test('Sidebar contains client-side read-only SQL precheck before submit', () => {
  const html = fs.readFileSync('src/Sidebar.html', 'utf8');
  assert.match(html, /function getClientReadOnlySqlError\(sql\)/);
  assert.match(html, /isClientForbiddenSqlKeyword\(keyword\)/);
  assert.match(html, /containsClientForbiddenSqlOperation\(statement\)/);
  assert.match(html, /containsClientReservedAuditSetStatement\(statement\)/);
  assert.match(html, /function quoteSqlIdentifier\(name\)/);
  assert.match(html, /function buildQualifiedTableName\(projectName, schemaName, tableName\)/);
  assert.match(html, /id="sqlInput"[\s\S]*?maxlength="65536"/);
  assert.match(html, /var MAX_SQL_LENGTH = 65536;/);
  assert.match(html, /errorSqlTooLong:\s*'SQL 长度超过限制（最多 65536 字符）。'/);
  assert.match(html, /errorSqlTooLong:\s*'SQL is too long\. Maximum length is 65536 characters\.'/);
  assert.match(html, /if \(sql\.length > MAX_SQL_LENGTH\) \{[\s\S]*?showError\(t\('errorSqlTooLong'\)\);[\s\S]*?return;/);
  assert.match(html, /var readOnlyError = getClientReadOnlySqlError\(sql\);[\s\S]*?\.submitQuery\(sql, null, targetSheet\);/);
});

test('Sidebar submit callbacks ignore stale aborted query responses', () => {
  const source = getSidebarScript();
  const runQuerySource = extractFunction(source, 'runQuery');

  assert.match(
    runQuerySource,
    /withSuccessHandler\(function\(submitResult\) \{[\s\S]*?if \(pollAborted\) return;/
  );
  assert.match(
    runQuerySource,
    /withFailureHandler\(function\(error\) \{[\s\S]*?if \(pollAborted\) return;[\s\S]*?setRunning\(false\);[\s\S]*?showError\(getSafeServerErrorMessage\(error\)\);/
  );
});

test('HTML google.script.run calls use allowlisted production callables', () => {
  const allowed = new Set(ALLOWED_PUBLIC_FUNCTIONS);
  const calls = [];

  for (const file of HTML_FILES) {
    const source = normalizeAppsScriptTemplates(extractScripts(fs.readFileSync(file, 'utf8')).join('\n'));
    for (const name of extractGoogleScriptRunCallNames(source)) {
      calls.push({ file, name });
    }
  }

  assert.deepEqual(
    calls.filter((call) => !allowed.has(call.name)),
    []
  );
  assert.deepEqual(
    Array.from(new Set(calls.map((call) => call.name))).sort(),
    [
      'appendInstanceHistory',
      'appendSqlHistory',
      'cancelQuery',
      'clearSqlHistory',
      'getConnectionStatus',
      'getMcConfigForUi',
      'getPartitions',
      'getQueryHistory',
      'getQueryProgress',
      'getSchemas',
      'getSheetNames',
      'getTableDetail',
      'getTables',
      'getUserLanguage',
      'removeSqlHistoryAt',
      'saveMcConfig',
      'setSqlHistoryEnabled',
      'submitQuery',
      'testConnection',
      'testMcConnection',
      'writeQueryResult'
    ]
  );
});

test('Sidebar loading text uses textContent unless explicitly rendering trusted HTML', () => {
  const source = getSidebarScript();
  const showLoadingSource = extractFunction(source, 'showLoading');
  const showLoadingHtmlSource = extractFunction(source, 'showLoadingHtml');

  assert.match(showLoadingSource, /\.textContent\s*=\s*text/);
  assert.doesNotMatch(showLoadingSource, /\.innerHTML/);
  assert.match(showLoadingHtmlSource, /\.innerHTML\s*=\s*html/);
});

test('Sidebar and Settings UI avoid emoji-only decoration', () => {
  const sidebarHtml = stripHtmlComments(fs.readFileSync('src/Sidebar.html', 'utf8'));
  const settingsHtml = stripHtmlComments(fs.readFileSync('src/Settings.html', 'utf8'));
  const combined = sidebarHtml + '\n' + settingsHtml;
  const emojiPattern = /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u;

  assert.doesNotMatch(combined, emojiPattern);
  assert.match(sidebarHtml, /btnRun[\s\S]*?>\s*执行查询\s*<\/button>/);
  assert.match(sidebarHtml, /tabQuery:\s*'SQL 查询'/);
  assert.match(sidebarHtml, /tabCatalog:\s*'数据目录'/);
  assert.match(sidebarHtml, /catalogSearch:\s*'搜索表名\.\.\.'/);
  assert.match(settingsHtml, /<h2 id="title">连接设置<\/h2>/);
  assert.match(settingsHtml, /btnSave:\s*'Save'/);
  assert.match(settingsHtml, /toggleHide:\s*'Show'/);
});

test('Sidebar catalog SQL helpers quote identifiers used in generated SQL', () => {
  const source = getSidebarScript();
  const generateSource = extractFunction(source, 'generateAndInsertSql');
  const helperSource = [
    extractFunction(source, 'quoteSqlIdentifier'),
    extractFunction(source, 'quoteSqlString'),
    extractFunction(source, 'buildQualifiedTableName'),
    extractFunction(source, 'buildMaxPtTableName')
  ].join('\n');
  const helpers = new Function(`${helperSource}\nreturn { quoteSqlIdentifier, quoteSqlString, buildQualifiedTableName, buildMaxPtTableName };`)();

  assert.equal(helpers.quoteSqlIdentifier('select'), '`select`');
  assert.equal(helpers.quoteSqlIdentifier('a`b'), '`a``b`');
  assert.equal(helpers.quoteSqlString("schema.o'hara"), "'schema.o''hara'");
  assert.equal(
    helpers.buildQualifiedTableName('project-1', 'schema.name', 'order'),
    '`project-1`.`schema.name`.`order`'
  );
  assert.equal(
    helpers.buildMaxPtTableName('', 'schema.name', "order's"),
    "schema.name.order's"
  );
  assert.match(generateSource, /buildQualifiedTableName\('',\s*schemaName,\s*tableName\)/);
  assert.doesNotMatch(generateSource, /projectBadge|projectName/);
});

test('Sidebar generated partition SQL uses MAX_PT only for single-level partitions', () => {
  const source = getSidebarScript();
  const helpers = new Function(
    'document',
    'switchTab',
    [
      extractFunction(source, 'quoteSqlIdentifier'),
      extractFunction(source, 'quoteSqlString'),
      extractFunction(source, 'buildQualifiedTableName'),
      extractFunction(source, 'buildMaxPtTableName'),
      extractFunction(source, 'buildLatestPartitionPredicate'),
      extractFunction(source, 'buildPartitionMaxSubquery'),
      extractFunction(source, 'generateAndInsertSql'),
      'return { generateAndInsertSql, buildLatestPartitionPredicate };'
    ].join('\n')
  )(
    {
      getElementById() {
        return {
          value: '',
          focus() {}
        };
      }
    },
    () => {}
  );
  const sqlInput = { value: '', focus() {} };
  const document = {
    getElementById() {
      return sqlInput;
    }
  };
  const run = new Function(
    'document',
    'switchTab',
    [
      extractFunction(source, 'quoteSqlIdentifier'),
      extractFunction(source, 'quoteSqlString'),
      extractFunction(source, 'buildQualifiedTableName'),
      extractFunction(source, 'buildMaxPtTableName'),
      extractFunction(source, 'buildLatestPartitionPredicate'),
      extractFunction(source, 'buildPartitionMaxSubquery'),
      extractFunction(source, 'generateAndInsertSql'),
      'return function(schemaName, tableName, detail) { generateAndInsertSql(schemaName, tableName, detail); return document.getElementById("sqlInput").value; };'
    ].join('\n')
  )(document, () => {});

  const singlePtSql = run('s1', 'orders', {
    columns: [{ name: 'id' }],
    partitionColumns: [{ name: 'ds' }]
  });
  assert.match(singlePtSql, /`ds` = MAX_PT\('s1\.orders'\)/);

  const multiPtSql = run('s1', 'orders', {
    columns: [{ name: 'id' }],
    partitionColumns: [{ name: 'ds' }, { name: 'hh' }]
  });
  assert.doesNotMatch(multiPtSql, /MAX_PT/);
  assert.match(multiPtSql, /`ds` = \(SELECT MAX\(`ds`\) FROM `s1`\.`orders`\)/);
  assert.match(multiPtSql, /`hh` = \(SELECT MAX\(`hh`\) FROM `s1`\.`orders` WHERE `ds` = \(SELECT MAX\(`ds`\) FROM `s1`\.`orders`\)\)/);

  assert.equal(
    helpers.buildLatestPartitionPredicate('`s1`.`orders`', [{ name: 'ds' }, { name: 'hh' }]),
    '`ds` = (SELECT MAX(`ds`) FROM `s1`.`orders`) AND `hh` = (SELECT MAX(`hh`) FROM `s1`.`orders` WHERE `ds` = (SELECT MAX(`ds`) FROM `s1`.`orders`))'
  );
});

test('Sidebar catalog schema loading has in-flight guard', () => {
  const source = getSidebarScript();
  const loadCatalogSource = extractFunction(source, 'loadCatalog');
  const refreshCatalogSource = extractFunction(source, 'refreshCatalog');

  assert.match(source, /var catalogLoadingInFlight = false;/);
  assert.match(loadCatalogSource, /if \(catalogLoadingInFlight\) return;/);
  assert.match(loadCatalogSource, /catalogLoadingInFlight = true;/);
  assert.match(loadCatalogSource, /withSuccessHandler\(function\(schemas\) \{[\s\S]*?catalogLoadingInFlight = false;/);
  assert.match(loadCatalogSource, /withFailureHandler\(function\(error\) \{[\s\S]*?catalogLoadingInFlight = false;/);
  assert.match(refreshCatalogSource, /catalogLoadingInFlight = false;/);
});

test('Sidebar catalog lazy loaders have keyed in-flight guards', () => {
  const source = getSidebarScript();
  const refreshCatalogSource = extractFunction(source, 'refreshCatalog');
  const renderCatalogErrorSource = extractFunction(source, 'renderCatalogError');
  const loadTablesSource = extractFunction(source, 'loadTablesForSchema');
  const loadDetailSource = extractFunction(source, 'loadTableDetail');
  const loadPartitionsSource = extractFunction(source, 'loadAndTogglePartitions');
  const insertTableSource = extractFunction(source, 'insertTableToSql');

  assert.match(source, /var catalogData = createCatalogData\(\);/);
  assert.match(source, /function createCatalogMap\(\)/);
  assert.match(source, /return Object\.create\(null\);/);
  assert.match(source, /var catalogTablesLoading = createCatalogMap\(\);/);
  assert.match(source, /var catalogDetailsLoading = createCatalogMap\(\);/);
  assert.match(source, /var catalogPartitionsLoading = createCatalogMap\(\);/);
  assert.match(refreshCatalogSource, /catalogData = createCatalogData\(\);/);
  assert.match(refreshCatalogSource, /catalogTablesLoading = createCatalogMap\(\);/);
  assert.match(refreshCatalogSource, /catalogDetailsLoading = createCatalogMap\(\);/);
  assert.match(refreshCatalogSource, /catalogPartitionsLoading = createCatalogMap\(\);/);
  assert.match(renderCatalogErrorSource, /if \(!container\) \{[\s\S]*?showError\(getSafeServerErrorMessage\(error\)\);[\s\S]*?return;/);

  assert.match(loadTablesSource, /if \(catalogTablesLoading\[schemaName\]\) return;/);
  assert.match(loadTablesSource, /catalogTablesLoading\[schemaName\] = true;/);
  assert.match(loadTablesSource, /delete catalogTablesLoading\[schemaName\];/);

  assert.match(loadDetailSource, /if \(catalogDetailsLoading\[tableKey\]\) return;/);
  assert.match(loadDetailSource, /catalogDetailsLoading\[tableKey\] = true;/);
  assert.match(loadDetailSource, /delete catalogDetailsLoading\[tableKey\];/);

  assert.match(loadPartitionsSource, /if \(catalogPartitionsLoading\[tableKey\]\) return;/);
  assert.match(loadPartitionsSource, /catalogPartitionsLoading\[tableKey\] = true;/);
  assert.match(loadPartitionsSource, /delete catalogPartitionsLoading\[tableKey\];/);

  assert.match(insertTableSource, /if \(catalogDetailsLoading\[tableKey\]\) return;/);
  assert.match(insertTableSource, /catalogDetailsLoading\[tableKey\] = true;/);
  assert.match(insertTableSource, /delete catalogDetailsLoading\[tableKey\];/);
  assert.match(insertTableSource, /renderCatalogError\(/);
  assert.doesNotMatch(insertTableSource, /generateAndInsertSql\(schemaName,\s*tableName,\s*null\)/);
});

test('Sidebar catalog state maps use null prototypes for user-controlled names', () => {
  const source = getSidebarScript();
  const helperSource = [
    extractFunction(source, 'createCatalogMap'),
    extractFunction(source, 'createCatalogData')
  ].join('\n');
  const helpers = new Function(`${helperSource}\nreturn { createCatalogMap, createCatalogData };`)();

  const map = helpers.createCatalogMap();
  map.__proto__ = 'schema-cache-value';
  map.constructor = 'table-cache-value';

  assert.equal(Object.getPrototypeOf(map), null);
  assert.equal(map.__proto__, 'schema-cache-value');
  assert.equal(map.constructor, 'table-cache-value');

  const data = helpers.createCatalogData();
  assert.equal(Object.getPrototypeOf(data.tables), null);
  assert.equal(Object.getPrototypeOf(data.details), null);
});

test('Sidebar catalog async responses are ignored after refresh version changes', () => {
  const source = getSidebarScript();
  const refreshCatalogSource = extractFunction(source, 'refreshCatalog');
  const loadCatalogSource = extractFunction(source, 'loadCatalog');
  const loadTablesSource = extractFunction(source, 'loadTablesForSchema');
  const loadDetailSource = extractFunction(source, 'loadTableDetail');
  const loadPartitionsSource = extractFunction(source, 'loadAndTogglePartitions');
  const insertTableSource = extractFunction(source, 'insertTableToSql');

  assert.match(source, /var catalogRequestVersion = 0;/);
  assert.match(refreshCatalogSource, /catalogRequestVersion\+\+;/);

  for (const fnSource of [
    loadCatalogSource,
    loadTablesSource,
    loadDetailSource,
    loadPartitionsSource,
    insertTableSource
  ]) {
    assert.match(fnSource, /var requestVersion = catalogRequestVersion;/);
    assert.match(fnSource, /if \(requestVersion !== catalogRequestVersion\) return;/);
  }
});

test('Sidebar client Instance ID guard matches backend format boundary', () => {
  const source = getSidebarScript();
  const guardSource = [
    'function t(key) { return key; }',
    extractFunction(source, 'getClientInstanceIdError')
  ].join('\n');
  const getClientInstanceIdError = new Function(`${guardSource}\nreturn getClientInstanceIdError;`)();

  assert.equal(getClientInstanceIdError('inst-1_2:3.4'), '');
  assert.equal(getClientInstanceIdError(''), 'errorEmptyInstanceId');
  assert.equal(getClientInstanceIdError('bad/id'), 'errorInvalidInstanceId');
  assert.equal(getClientInstanceIdError('bad..id'), 'errorInvalidInstanceId');
  assert.equal(getClientInstanceIdError('a'.repeat(129)), 'errorInvalidInstanceId');
});

test('Sidebar failure status messages summarize Instance ID length', () => {
  const source = getSidebarScript();
  const startPollingSource = extractFunction(source, 'startPolling');
  const attachSource = extractFunction(source, 'attachToJob');
  const cancelSource = extractFunction(source, 'cancelQueryClick');
  const summarySource = extractFunction(source, 'getInstanceIdDisplaySummary');

  assert.match(source, /instanceIdLenLabel:\s*'Instance ID 长度'/);
  assert.match(source, /instanceIdLenLabel:\s*'Instance ID length'/);
  assert.match(summarySource, /String\(instanceId \|\| ''\)\.length/);
  for (const fnSource of [startPollingSource, attachSource, cancelSource]) {
    assert.match(fnSource, /getInstanceIdDisplaySummary\(instanceId\)/);
    assert.doesNotMatch(fnSource, /Instance: ' \+ instanceId|Instance ID: ' \+ instanceId|\+ instanceId \+ '\)/);
  }
});

test('Sidebar cancel failure surfaces raw kill result for traceability', () => {
  const source = getSidebarScript();
  const cancelSource = extractFunction(source, 'cancelQueryClick');
  const isFailureSource = extractFunction(source, 'isCancelKillFailure');
  const safeResultSource = extractFunction(source, 'getSafeCancelKillResult');
  const summarySource = extractFunction(source, 'getInstanceIdDisplaySummary');

  let successHandler = null;
  const btnCancel = { disabled: false, textContent: '', style: {} };
  const state = new Function(
    'document',
    'google',
    'window',
    [
      'var currentInstanceId = "sensitive-instance-id";',
      'function t(key) { return ({ btnCancelling: "Cancelling...", loadingCancelling: "Killing instance...", btnCancel: "Cancel", cancelRequestFailed: "Cancel request failed. Please retry", instanceIdLenLabel: "Instance ID length" })[key] || key; }',
      'var state = { loadingText: "", errorText: "" };',
      'function showLoading(text) { state.loadingText = text; }',
      'function showError(text) { state.errorText = text; }',
      summarySource,
      isFailureSource,
      safeResultSource,
      cancelSource,
      'cancelQueryClick();',
      'return state;'
    ].join('\n')
  )(
    { getElementById: (id) => id === 'btnCancel' ? btnCancel : {} },
    {
      script: {
        run: {
          withSuccessHandler(fn) {
            successHandler = fn;
            return this;
          },
          withFailureHandler() {
            return this;
          },
          cancelQuery() {
            return this;
          }
        }
      }
    },
    { console: { log() {}, warn() {} } }
  );

  assert.equal(btnCancel.disabled, true);
  assert.equal(btnCancel.textContent, 'Cancelling...');
  assert.match(state.loadingText, /Instance ID length: 21/);
  assert.equal(typeof successHandler, 'function');

  const result = successHandler({ killResult: 'failed:raw sensitive_project sensitive_table', instanceId: 'sensitive-instance-id' });

  assert.equal(result, undefined);
  assert.equal(btnCancel.disabled, false);
  assert.equal(btnCancel.textContent, 'Cancel');
  assert.equal(state.loadingText, 'Instance ID length: 21 - Waiting for completion...');
  assert.equal(state.errorText, 'Cancel request failed. Please retry: failed:raw sensitive_project sensitive_table');
});

test('Sidebar cancel failure result normalizes whitespace and caps very long messages', () => {
  const source = getSidebarScript();
  const safeResultSource = extractFunction(source, 'getSafeCancelKillResult');
  const getSafeCancelKillResult = new Function(`${safeResultSource}\nreturn getSafeCancelKillResult;`)();

  assert.equal(getSafeCancelKillResult({ killResult: 'failed:simple' }), 'failed:simple');
  assert.equal(getSafeCancelKillResult({ killResult: 'failed:\nwith\tnewlines' }), 'failed: with newlines');
  assert.equal(getSafeCancelKillResult({ killResult: 'failed:multi   spaces' }), 'failed:multi spaces');

  const longResult = 'failed:' + 'x'.repeat(500);
  const capped = getSafeCancelKillResult({ killResult: longResult });
  assert.equal(capped.length, 203);
  assert.ok(capped.endsWith('...'));
  assert.ok(capped.startsWith('failed:'));
});

test('Sidebar server error formatter passes raw messages through for traceability', () => {
  const source = getSidebarScript();
  const formatterSource = extractFunction(source, 'getSafeServerErrorMessage');
  const getSafeServerErrorMessage = new Function(`${formatterSource}\nreturn getSafeServerErrorMessage;`)();

  // Short messages pass through unchanged.
  assert.equal(getSafeServerErrorMessage({ message: 'Forbidden: access denied on project_a' }), 'Forbidden: access denied on project_a');
  assert.equal(getSafeServerErrorMessage({ message: 'HTTP 403' }), 'HTTP 403');
  assert.equal(getSafeServerErrorMessage({ message: 'SQL 长度超过限制（最多 65536 字符）。' }), 'SQL 长度超过限制（最多 65536 字符）。');
  assert.equal(
    getSafeServerErrorMessage({ message: '提交作业失败 (HTTP 403): denied on customer_project.orders' }),
    '提交作业失败 (HTTP 403): denied on customer_project.orders'
  );
  assert.equal(
    getSafeServerErrorMessage({ message: '获取表列表失败 (HTTP 403) requestId=req-table-1: NoSuchObject: table missing' }),
    '获取表列表失败 (HTTP 403) requestId=req-table-1: NoSuchObject: table missing'
  );

  // Null / undefined inputs do not throw.
  assert.equal(getSafeServerErrorMessage(null), '');
  assert.equal(getSafeServerErrorMessage(undefined), '');

  // Newlines and tabs are normalized to single spaces for single-line toasts.
  assert.equal(
    getSafeServerErrorMessage({ message: 'line1\nline2\tline3' }),
    'line1 line2 line3'
  );
  assert.equal(
    getSafeServerErrorMessage({ message: 'multi   internal     spaces' }),
    'multi internal spaces'
  );

  // Very long messages are capped at 500 chars with a trailing ellipsis.
  const longMessage = 'denied: ' + 'x'.repeat(700);
  const capped = getSafeServerErrorMessage({ message: longMessage });
  assert.equal(capped.length, 503);
  assert.ok(capped.endsWith('...'));
  assert.ok(capped.startsWith('denied: '));
});

test('Sidebar query history can be cleared from localStorage with confirmation', () => {
  const html = fs.readFileSync('src/Sidebar.html', 'utf8');
  const source = getSidebarScript();
  const applyLanguageSource = extractFunction(source, 'applyLanguage');
  const clearHistorySource = extractFunction(source, 'clearHistory');
  const clearHistoryStorageSource = extractFunction(source, 'clearHistoryStorage');

  assert.match(html, /id="btnClearHistory"/);
  assert.match(html, /id="historyEnabled"/);
  assert.match(html, /id="historyEnabledLabel"/);
  assert.match(source, /historyClear:\s*'清空历史'/);
  assert.match(source, /historyClear:\s*'Clear history'/);
  assert.match(source, /historyClearConfirm:\s*'确定清空最近查询历史吗？'/);
  assert.match(source, /historyClearConfirm:\s*'Clear recent query history\?'/);
  assert.match(source, /historyEnabled:\s*'保存本地历史'/);
  assert.match(source, /historyEnabled:\s*'Save local history'/);

  const elements = {
    sqlLabel: {},
    sqlInput: {},
    projectLabel: {},
    projectName: {},
    targetSheetLabel: {},
    btnRun: {},
    btnCancel: {},
    btnTest: {},
    loadingText: {},
    attachToggleLabel: {},
    attachInstanceId: {},
    btnAttach: {},
    historyEnabledLabel: {},
    historyEnabled: { checked: true },
    tabQueryButton: {},
    tabCatalogButton: {},
    catalogSearch: {},
    catalogLoading: {},
    historyTitle: {},
    btnClearHistory: {
      attrs: {},
      setAttribute(name, value) {
        this.attrs[name] = value;
      }
    }
  };
  let removedKey = '';
  let renderedList = null;
  let confirmMessage = '';
  const clearHistory = new Function(
    'document',
    'localStorage',
    'confirm',
    'renderHistory',
    [
      "var currentLang = 'en';",
      'var HISTORY_KEY = "mc_sql_history";',
      'var i18n = { en: { historyLabel: "Recent Queries", historyClear: "Clear history", historyClearButton: "Clear", historyClearConfirm: "Clear recent query history?", historyEnabled: "Save local history", sqlLabel: "", sqlPlaceholder: "", projectLabel: "", projectPlaceholder: "", targetSheetLabel: "", btnRun: "", btnCancel: "", btnTest: "", loadingExecuting: "", attachToggle: "", attachPlaceholder: "", attachButton: "", tabQuery: "", tabCatalog: "", catalogSearch: "", catalogLoading: "", catalogRefresh: "Refresh" } };',
      'function t(key) { return i18n[currentLang][key] || key; }',
      'function updateLanguageUI() { document.getElementById("historyTitle").textContent = t("historyLabel"); document.getElementById("btnClearHistory").title = t("historyClear"); document.getElementById("btnClearHistory").setAttribute("aria-label", t("historyClear")); document.getElementById("btnClearHistory").textContent = t("historyClearButton"); document.getElementById("historyEnabledLabel").textContent = t("historyEnabled"); }',
      'function mirrorHistoryCall_() {}',
      applyLanguageSource,
      clearHistoryStorageSource,
      clearHistorySource,
      'applyLanguage("en");',
      'return clearHistory;'
    ].join('\n')
  )(
    { getElementById: (id) => elements[id] || {} },
    { removeItem: (key) => { removedKey = key; } },
    (message) => {
      confirmMessage = message;
      return true;
    },
    (list) => { renderedList = list; }
  );

  assert.equal(elements.historyTitle.textContent, 'Recent Queries');
  assert.equal(elements.btnClearHistory.title, 'Clear history');
  assert.equal(elements.btnClearHistory.attrs['aria-label'], 'Clear history');
  assert.equal(elements.btnClearHistory.textContent, 'Clear');
  assert.equal(elements.historyEnabledLabel.textContent, 'Save local history');

  clearHistory();

  assert.equal(confirmMessage, 'Clear recent query history?');
  assert.equal(removedKey, 'mc_sql_history');
  assert.deepEqual(renderedList, []);
});

test('Sidebar local SQL history can be disabled to avoid saving future queries', () => {
  const source = getSidebarScript();
  const storage = {};
  const renderedLists = [];
  const elements = {
    historyEnabled: { checked: false },
    historyList: {
      innerHTML: '',
      children: [],
      appendChild(child) {
        this.children.push(child);
      }
    },
    historySection: { style: {} }
  };
  const helpers = new Function(
    'document',
    'localStorage',
    [
      'var HISTORY_KEY = "mc_sql_history";',
      'var HISTORY_ENABLED_KEY = "mc_sql_history_enabled";',
      'var MAX_HISTORY = 10;',
      'function t(key) { return key === "historyDisabled" ? "Local history is off" : key; }',
      'function mirrorHistoryCall_() {}',
      extractFunction(source, 'saveToHistory'),
      extractFunction(source, 'toggleHistoryEnabled'),
      extractFunction(source, 'applyHistoryPreference'),
      extractFunction(source, 'isHistoryEnabled'),
      extractFunction(source, 'setHistoryEnabled'),
      extractFunction(source, 'clearHistoryStorage'),
      extractFunction(source, 'getHistoryList'),
      extractFunction(source, 'renderHistory'),
      'return { saveToHistory, toggleHistoryEnabled, applyHistoryPreference, isHistoryEnabled, getHistoryList };'
    ].join('\n')
  )(
    {
      getElementById: (id) => elements[id] || {},
      createElement: () => ({
        dataset: {},
        children: [],
        addEventListener() {},
        appendChild(child) {
          this.children.push(child);
        }
      })
    },
    {
      getItem: (key) => Object.prototype.hasOwnProperty.call(storage, key) ? storage[key] : null,
      setItem: (key, value) => { storage[key] = String(value); },
      removeItem: (key) => { delete storage[key]; }
    }
  );

  storage.mc_sql_history = JSON.stringify(['select secret from t']);
  helpers.toggleHistoryEnabled();

  assert.equal(storage.mc_sql_history_enabled, 'false');
  assert.equal(storage.mc_sql_history, undefined);
  assert.equal(helpers.isHistoryEnabled(), false);
  assert.equal(elements.historySection.style.display, 'block');
  assert.match(elements.historyList.innerHTML, /Local history is off/);

  helpers.saveToHistory('select should_not_persist');
  assert.equal(storage.mc_sql_history, undefined);

  elements.historyEnabled.checked = true;
  helpers.toggleHistoryEnabled();
  helpers.saveToHistory('select 1');

  assert.equal(storage.mc_sql_history_enabled, 'true');
  assert.deepEqual(JSON.parse(storage.mc_sql_history), ['select 1']);
});

test('Sidebar instance history feeds visible attach select with one-day TTL', () => {
  const html = fs.readFileSync('src/Sidebar.html', 'utf8');
  const source = getSidebarScript();
  const storage = {};
  let now = 1_700_000_000_000;
  const select = {
    children: [],
    classList: {
      visible: false,
      toggle(name, value) {
        if (name === 'visible') this.visible = !!value;
      }
    },
    value: '',
    set innerHTML(value) {
      this._innerHTML = value;
      this.children = [];
    },
    get innerHTML() {
      return this._innerHTML || '';
    },
    appendChild(child) {
      this.children.push(child);
    }
  };

  const input = { value: '' };

  assert.match(html, /id="attachInstanceSelect"/);
  assert.doesNotMatch(getTagById(html, 'attachInstanceId'), /list=/);
  assert.match(source, /var INSTANCE_HISTORY_TTL_MS = 24 \* 60 \* 60 \* 1000;/);

  const helpers = new Function(
    'document',
    'localStorage',
    'Date',
    [
      'var MAX_HISTORY = 10;',
      'var INSTANCE_HISTORY_KEY = "mc_instance_history";',
      'var INSTANCE_HISTORY_TTL_MS = 24 * 60 * 60 * 1000;',
      'function t(key) { return key; }',
      'function mirrorHistoryCall_() {}',
      extractFunction(source, 'getClientInstanceIdError'),
      extractFunction(source, 'saveInstanceToHistory'),
      extractFunction(source, 'getInstanceHistoryList'),
      extractFunction(source, 'normalizeInstanceHistoryItem'),
      extractFunction(source, 'persistInstanceHistoryList'),
      extractFunction(source, 'renderInstanceHistory'),
      extractFunction(source, 'selectAttachInstanceHistory'),
      'return { saveInstanceToHistory, getInstanceHistoryList, renderInstanceHistory, selectAttachInstanceHistory };'
    ].join('\n')
  )(
    {
      getElementById: (id) => {
        if (id === 'attachInstanceSelect') return select;
        if (id === 'attachInstanceId') return input;
        return null;
      },
      createElement: () => ({ value: '', textContent: '' })
    },
    {
      getItem: (key) => Object.prototype.hasOwnProperty.call(storage, key) ? storage[key] : null,
      setItem: (key, value) => { storage[key] = String(value); },
      removeItem: (key) => { delete storage[key]; }
    },
    {
      now: () => now
    }
  );

  helpers.saveInstanceToHistory('inst-1');
  now += 1000;
  helpers.saveInstanceToHistory('inst-2');
  now += 1000;
  helpers.saveInstanceToHistory('inst-1');

  assert.deepEqual(JSON.parse(storage.mc_instance_history).map((item) => item.instanceId), ['inst-1', 'inst-2']);
  assert.equal(select.classList.visible, true);
  assert.deepEqual(select.children.map((item) => item.value), ['', 'inst-1', 'inst-2']);
  assert.deepEqual(select.children.map((item) => item.textContent), ['attachRecentJobs', 'inst-1', 'inst-2']);

  select.value = 'inst-2';
  helpers.selectAttachInstanceHistory();
  assert.equal(input.value, 'inst-2');

  storage.mc_instance_history = JSON.stringify([
    { instanceId: 'fresh-1', savedAt: now - 1000 },
    { instanceId: 'old-1', savedAt: now - (24 * 60 * 60 * 1000) - 1 },
    { instanceId: '../bad', savedAt: now }
  ]);

  assert.deepEqual(helpers.getInstanceHistoryList(now).map((item) => item.instanceId), ['fresh-1']);
});

test('Sidebar success summary escapes server-provided fields before rendering HTML', () => {
  const source = getSidebarScript();
  const showSuccessSource = extractFunction(source, 'showSuccess');
  const formatResultMetricSource = extractFunction(source, 'formatResultMetric');
  const formatMsSource = extractFunction(source, 'formatMs');
  const escapeHtmlSource = extractFunction(source, 'escapeHtml');
  let rendered = null;

  const showSuccess = new Function(
    'document',
    [
      "function t(key) { return ({ resultRows: 'Rows', resultColumns: 'Columns', resultTime: 'Time', resultWrite: 'Written', resultSuccess: 'Done' })[key] || key; }",
      'function showResult(type, title, bodyHtml) { rendered = { type: type, title: title, bodyHtml: bodyHtml }; }',
      'var rendered;',
      escapeHtmlSource,
      formatMsSource,
      formatResultMetricSource,
      showSuccessSource,
      'return function(summary, executionTimeMs) { showSuccess(summary, executionTimeMs); return rendered; };'
    ].join('\n')
  )({
    createElement: () => {
      let value = '';
      return {
        set textContent(next) {
          value = String(next)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
        },
        get innerHTML() {
          return value;
        }
      };
    }
  });

  rendered = showSuccess({
    rowCount: '<img src=x onerror=alert(1)>',
    columnCount: '<script>alert(2)</script>',
    sheetName: 'Result <b onclick=alert(3)>x</b>',
    instanceId: 'inst"><img src=x onerror=alert(4)>',
    logviewUrl: 'https://example.com/?q="><script>alert(5)</script>'
  }, 1234);

  assert.equal(rendered.type, 'success');
  assert.equal(rendered.title, 'Done');
  assert.doesNotMatch(rendered.bodyHtml, /<script|<img/i);
  assert.doesNotMatch(rendered.bodyHtml, /href="https:\/\/example\.com\/\?q=">/);
  assert.match(rendered.bodyHtml, /&lt;script&gt;alert\(2\)&lt;\/script&gt;/);
  assert.match(rendered.bodyHtml, /Result &lt;b onclick=alert\(3\)&gt;x&lt;\/b&gt;/);
  assert.match(rendered.bodyHtml, /inst&quot;&gt;&lt;img src=x onerror=alert\(4\)&gt;/);
  assert.match(rendered.bodyHtml, /href="https:\/\/example\.com\/\?q=&quot;&gt;&lt;script&gt;alert\(5\)&lt;\/script&gt;"/);
});

test('Settings client endpoint guard matches backend endpoint policy', () => {
  const source = getSettingsScript();
  const guardSource = [
    'function t(key) { return key; }',
    extractFunction(source, 'getClientEndpointError')
  ].join('\n');
  const getClientEndpointError = new Function(`${guardSource}\nreturn getClientEndpointError;`)();

  assert.equal(getClientEndpointError('https://service.cn-hangzhou.maxcompute.aliyun.com/api'), '');
  assert.equal(getClientEndpointError(''), 'errorRequired');
  assert.equal(getClientEndpointError('http://service.cn-hangzhou.maxcompute.aliyun.com/api'), 'errorInvalidEndpoint');
  assert.equal(getClientEndpointError('https://example.com/api'), 'errorInvalidEndpoint');
  assert.equal(getClientEndpointError('https://service.cn-hangzhou.maxcompute.aliyun.com/api/'), 'errorInvalidEndpoint');
});

test('Settings initial config load reports safe failure message', () => {
  const source = getSettingsScript();
  const loadCurrentSettingsSource = extractFunction(source, 'loadCurrentSettings');

  assert.match(
    loadCurrentSettingsSource,
    /withFailureHandler\(function\(err\) \{[\s\S]*?showResult\('error', getSafeServerErrorMessage\(err\)\);[\s\S]*?\}\)[\s\S]*?\.getMcConfigForUi\(\);/
  );
});

test('Settings sensitive credential inputs use hidden non-spellchecked fields', () => {
  const html = fs.readFileSync('src/Settings.html', 'utf8');
  const accessKeyId = getTagById(html, 'accessKeyId');
  const accessKeySecret = getTagById(html, 'accessKeySecret');
  const securityToken = getTagById(html, 'securityToken');
  const project = getTagById(html, 'project');
  const customEndpoint = getTagById(html, 'customEndpoint');

  assert.match(accessKeyId, /autocomplete="off"/);
  assert.match(accessKeyId, /spellcheck="false"/);
  assert.match(accessKeyId, /maxlength="128"/);
  assert.match(accessKeySecret, /type="password"/);
  assert.match(accessKeySecret, /autocomplete="new-password"/);
  assert.match(accessKeySecret, /spellcheck="false"/);
  assert.match(accessKeySecret, /maxlength="256"/);
  assert.match(securityToken, /type="password"/);
  assert.match(securityToken, /autocomplete="new-password"/);
  assert.match(securityToken, /spellcheck="false"/);
  assert.match(securityToken, /maxlength="4096"/);
  assert.match(project, /autocomplete="off"/);
  assert.match(project, /spellcheck="false"/);
  assert.match(project, /maxlength="128"/);
  assert.match(customEndpoint, /autocomplete="off"/);
  assert.match(customEndpoint, /spellcheck="false"/);
  assert.match(customEndpoint, /maxlength="256"/);
  assert.match(html, /toggleSensitiveField\('accessKeySecret', 'toggleSecretBtn'\)/);
  assert.match(html, /toggleSensitiveField\('securityToken', 'toggleTokenBtn'\)/);
});

test('Settings client config length guard matches backend field limits', () => {
  const source = getSettingsScript();
  const guardSource = [
    'var CONFIG_FIELD_LIMITS = { accessKeyId: { label: "AccessKey ID", max: 128 }, accessKeySecret: { label: "AccessKey Secret", max: 256 }, project: { label: "Project", max: 128 }, endpoint: { label: "Endpoint", max: 256 }, securityToken: { label: "Security Token", max: 4096 } };',
    'function t(key) { return key === "errorFieldTooLong" ? "{field} is too long. Maximum length is {max} characters." : key; }',
    extractFunction(source, 'getClientConfigError')
  ].join('\n');
  const getClientConfigError = new Function(`${guardSource}\nreturn getClientConfigError;`)();

  assert.equal(getClientConfigError({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    securityToken: ''
  }), '');
  assert.equal(
    getClientConfigError({ accessKeyId: 'a'.repeat(129) }),
    'AccessKey ID is too long. Maximum length is 128 characters.'
  );
  assert.equal(
    getClientConfigError({ securityToken: 't'.repeat(4097) }),
    'Security Token is too long. Maximum length is 4096 characters.'
  );
});

test('Settings save and test precheck config field length before server calls', () => {
  const source = getSettingsScript();
  const saveSettingsSource = extractFunction(source, 'saveSettings');
  const testConnectionSource = extractFunction(source, 'testConnection');

  for (const fnSource of [saveSettingsSource, testConnectionSource]) {
    assert.match(fnSource, /var configError = getClientConfigError\(config\);/);
    assert.match(fnSource, /if \(configError\) \{[\s\S]*?showResult\('error', configError\);[\s\S]*?return;/);
  }
  assert.match(saveSettingsSource, /getClientConfigError\(config\);[\s\S]*?\.saveMcConfig\(config\);/);
  assert.match(testConnectionSource, /getClientConfigError\(config\);[\s\S]*?\.testMcConnection\(config\);/);
});

test('Settings sensitive field toggle supports secret and token inputs', () => {
  const source = getSettingsScript();
  const toggleSource = extractFunction(source, 'toggleSensitiveField');
  const elements = {
    accessKeySecret: { type: 'password' },
    toggleSecretBtn: { textContent: '' },
    securityToken: { type: 'text' },
    toggleTokenBtn: { textContent: '' }
  };
  const toggleSensitiveField = new Function(
    'document',
    't',
    `${toggleSource}\nreturn toggleSensitiveField;`
  )(
    { getElementById: (id) => elements[id] },
    (key) => key === 'toggleShow' ? 'hide-label' : 'show-label'
  );

  toggleSensitiveField('accessKeySecret', 'toggleSecretBtn');
  assert.equal(elements.accessKeySecret.type, 'text');
  assert.equal(elements.toggleSecretBtn.textContent, 'hide-label');

  toggleSensitiveField('securityToken', 'toggleTokenBtn');
  assert.equal(elements.securityToken.type, 'password');
  assert.equal(elements.toggleTokenBtn.textContent, 'show-label');
});

test('Settings server error formatter hides unexpected raw messages', () => {
  const source = getSettingsScript();
  const formatterSource = extractFunction(source, 'getSafeServerErrorMessage');
  const getSafeServerErrorMessage = new Function(`${formatterSource}\nreturn getSafeServerErrorMessage;`)();

  assert.equal(getSafeServerErrorMessage({ message: 'BadRequest: messageLen=14' }), 'BadRequest: messageLen=14');
  const rawSettingsFailure = 'raw settings failure mentions sensitive_project';
  assert.equal(
    getSafeServerErrorMessage({ message: rawSettingsFailure }),
    `Server error: messageLen=${rawSettingsFailure.length}`
  );
  assert.doesNotMatch(
    getSafeServerErrorMessage({ message: rawSettingsFailure }),
    /sensitive_project/
  );
});

test('Sidebar client read-only SQL guard behavior matches backend policy shape', () => {
  const source = getSidebarScript();
  const guardSource = [
    'function t(key) { return key; }',
    extractFunction(source, 'getClientReadOnlySqlError'),
    extractFunction(source, 'splitClientSqlStatements'),
    extractFunction(source, 'addClientSqlStatement'),
    extractFunction(source, 'getClientFirstSqlKeyword'),
    extractFunction(source, 'isClientAllowedReadOnlySqlKeyword'),
    extractFunction(source, 'isClientForbiddenSqlKeyword'),
    extractFunction(source, 'containsClientReservedAuditSetStatement'),
    extractFunction(source, 'shouldClientCheckNestedForbiddenSqlOperation'),
    extractFunction(source, 'containsClientForbiddenSqlOperation'),
    extractFunction(source, 'maskClientSqlCommentsAndLiterals')
  ].join('\n');
  const getClientReadOnlySqlError = new Function(`${guardSource}\nreturn getClientReadOnlySqlError;`)();

  assert.equal(getClientReadOnlySqlError('set odps.sql.mapper.split.size=256; select 1'), '');
  assert.equal(getClientReadOnlySqlError('with c as (select 1) select * from c'), '');
  assert.equal(getClientReadOnlySqlError("select 'drop table t' as text -- delete from x"), '');
  assert.equal(getClientReadOnlySqlError('show create table t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('show tables'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('desc t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('describe t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain select * from t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('drop table t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain insert into t select 1'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain create materialized view mv as select 1'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain create external table t (id bigint)'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain add file "oss://bucket/path.jar"'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain install package p'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('explain msck repair table t'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('analyze table t compute statistics'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('call some_proc()'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('use other_project'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('begin'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('commit'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError('rollback'), 'errorReadOnlySql');
  assert.equal(getClientReadOnlySqlError("set EXT_PLATFORM_ID='x'; select 1"), 'errorReservedAuditSet');
  assert.equal(getClientReadOnlySqlError('select 1; select 2'), 'errorSingleReadOnlyQuery');
  assert.equal(getClientReadOnlySqlError('select 1; set x=1'), 'errorSetBeforeQuery');
});
