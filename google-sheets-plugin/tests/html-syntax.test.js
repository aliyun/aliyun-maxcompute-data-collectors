const fs = require('node:fs');
const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
function sidebarFunctions(names, context = {}) {
  vm.createContext(context);
  vm.runInContext(names.map(name => extractFunction(getSidebarScript(), name)).join('\n'), context);
  return context;
}

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
  const source = getSidebarScript();
  const run = extractFunction(source, 'runCurrentSheet');
  assert.match(source, /var MAX_SQL_LENGTH = 65536;/);
  assert.match(fs.readFileSync('src/Sidebar.html', 'utf8'), /id="sqlInput"[\s\S]*?maxlength="65536"/);
  assert.ok(run.indexOf('domData.sql.length > MAX_SQL_LENGTH') < run.indexOf('.submitQuery('));
  assert.ok(run.indexOf('getClientReadOnlySqlError(domData.sql)') < run.indexOf('.submitQuery('));
  assert.match(run, /if \(readOnlyError\) \{[\s\S]*?return;/);
  assert.match(run, /if \(domData.sql.length > MAX_SQL_LENGTH\) \{[\s\S]*?return;/);
});

test('Sidebar submit callbacks ignore stale aborted query responses', () => {
  let success, failure, submitted, record;
  const state = {}, jobs = [], notices = [];
  const ctx = sidebarFunctions(['runCurrentSheet'], {
    isRunning: false, currentSheetId: 1, currentSheetName: 'Original', currentMode: 'sql', MAX_SQL_LENGTH: 65536,
    getCurrentSqlData: () => ({sql:'select 1', mode:'sql'}), getClientReadOnlySqlError: () => '',
    saveCurrentSql() {}, getOrCreateSheetState: () => state, updateExecStatusDom() {}, setInputsDisabled() {},
    saveToHistory() {}, saveInstanceToHistory() {}, persistJob() {}, startJobPolling() {}, renderJobs() {},
    recordJobFromSheet: (...args) => {record = args;}, jobList: jobs,
    showNotification: (...args) => notices.push(args), t: x => x, getSafeServerErrorMessage: e => e.message,
    google: {script: {run: {withSuccessHandler(fn) {success=fn; return this;},
      withFailureHandler(fn) {failure=fn; return this;}, submitQuery(...args) {submitted=args;}}}}
  });
  ctx.runCurrentSheet();
  ctx.currentSheetId = 2; ctx.currentSheetName = 'Other';
  success({sync:false, instanceId:'inst-1'});
  assert.equal(submitted[2], 'Original');
  assert.equal(jobs[0].targetSheet, 'Original');
  state.submissionToken = {}; // A superseding request owns this state now.
  const snapshot = JSON.stringify(state);
  failure({message:'stale failure'});
  success({sync:true, instanceId:'stale'});
  assert.equal(JSON.stringify(state), snapshot);
  assert.equal(record, undefined);
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
  assert.ok(calls.length > 30, 'expected current query, schedule and OSS RPC surface');

});

test('Sidebar loading text uses textContent unless explicitly rendering trusted HTML', () => {
  const source = getSidebarScript();
  assert.match(extractFunction(source, 'loadCatalog'), /loadingEl\.textContent = t\('catalogLoading'\)/);
  const el = {className:'', textContent:'', classList:{remove(){}}};
  const ctx = sidebarFunctions(['showNotification'], {document:{getElementById:()=>el}, notificationTimer:null, setTimeout:()=>1});
  ctx.showNotification('error', '<img onerror=alert(1)>');
  assert.equal(el.textContent, '<img onerror=alert(1)>');
  assert.equal(el.innerHTML, undefined);
});

test('Sidebar and Settings UI avoid emoji-only decoration', () => {
  const html = stripHtmlComments(fs.readFileSync('src/Sidebar.html', 'utf8'));
  assert.match(html, /id="labelRun">Run<\/span>/);
  assert.match(html, /id="btnClearHistory"[^>]*>Clear<\/button>/);
  assert.match(html, /id="labelInstanceId">Instance ID<\/label>/);
  const settings = fs.readFileSync('src/Settings.html', 'utf8');
  assert.match(settings, /id="btnSave"/);
  // Icons accompany labels or title attributes in the current SVG UI.
  assert.match(extractFunction(getSidebarScript(), 'getJobActionHtml'), /title="Cancel"/);
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
    'setMode',
    [
      'function switchTab() {}',
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
    'setMode',
    [
      'function switchTab() {}',
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
  const loadTablesSource = extractFunction(source, 'loadTablesForSchema');
  const loadDetailSource = extractFunction(source, 'loadTableDetailForPreview');
  const loadPartitionsSource = extractFunction(source, 'loadAndTogglePartitionsPreview');
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
  assert.match(insertTableSource, /showNotification\('error', getSafeServerErrorMessage\(error\)\)/);
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
  const loadDetailSource = extractFunction(source, 'loadTableDetailForPreview');
  const loadPartitionsSource = extractFunction(source, 'loadAndTogglePartitionsPreview');
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

test('Sidebar failure status escapes displayed Instance IDs', () => {
  // Jobs now intentionally show the submitting user's Instance ID. Error UI must escape it.
  const el = {style:{}, innerHTML:''};
  const ctx = sidebarFunctions(['escapeHtml','escapeAttr','formatMs','updateExecStatusDom'], {document:{getElementById:()=>el},t:x=>x});
  ctx.updateExecStatusDom({status:'failed',error:'inst"><img src=x onerror=alert(1)>'});
  assert.doesNotMatch(el.innerHTML, /<img/);
  assert.match(el.innerHTML, /&lt;img/);
});

test('Sidebar cancel failure surfaces raw kill result for traceability', () => {
  let success, failure, sent = 0;
  const job = {id:'job-1',instanceId:'inst-1',status:'running'}, notices=[];
  const ctx = sidebarFunctions(['cancelJobClick','getSafeCancelKillResult','getSafeServerErrorMessage'], {
    findJob:()=>job, showNotification:(...args)=>notices.push(args),t:x=>x,
    google:{script:{run:{withSuccessHandler(fn){success=fn;return this;},withFailureHandler(fn){failure=fn;return this;},cancelQuery(){sent++;}}}}
  });
  ctx.cancelJobClick('job-1'); ctx.cancelJobClick('job-1');
  assert.equal(sent,1);
  success({killResult:'failed:raw sensitive_project sensitive_table'});
  assert.equal(job.cancelPending,false);
  assert.equal(job.status,'running');
  assert.match(notices[0][1],/failed:raw sensitive_project sensitive_table/);
  ctx.cancelJobClick('job-1'); failure({message:'network failure'});
  assert.equal(job.cancelPending,false);
  assert.match(notices[1][1],/network failure/);
  assert.match(extractFunction(getSidebarScript(),'renderJobs'),/cancelBtn\.addEventListener\('click'/);
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
  let confirmed = false, removed = false, rendered, mirrored;
  const ctx = sidebarFunctions(['clearHistory'], {confirm:()=>confirmed, t:x=>x,
    historyRequestVersion:0, HISTORY_KEY:'history',localStorage:{removeItem(){removed=true;}},renderHistory:list=>{rendered=list;},mirrorHistoryCall_:fn=>{mirrored=fn;}});
  ctx.clearHistory(); assert.equal(removed,false);
  confirmed=true; ctx.clearHistory();
  assert.equal(removed,true); assert.equal(rendered.length,0); assert.equal(mirrored,'clearSqlHistory');
});

test('Sidebar local SQL history can be disabled to avoid saving future queries', () => {
  const storage = {history:'["select secret"]'}, calls=[], checkbox={checked:false};
  const ctx = sidebarFunctions(['toggleHistoryEnabled','setHistoryEnabled','isHistoryEnabled','getHistoryList','saveToHistory'], {
    document:{getElementById:()=>checkbox}, historyRequestVersion:0, HISTORY_KEY:'history', HISTORY_ENABLED_KEY:'enabled', MAX_HISTORY:10,
    localStorage:{getItem:k=>storage[k],setItem:(k,v)=>{storage[k]=v;},removeItem:k=>{delete storage[k];}},
    mirrorHistoryCall_:(...args)=>calls.push(args),applyHistoryPreference(){},loadHistory(){},renderHistory(){}
  });
  ctx.toggleHistoryEnabled();
  assert.equal(storage.enabled,'false'); assert.equal(storage.history,undefined);
  ctx.saveToHistory('select should_not_persist'); assert.equal(storage.history,undefined);
  assert.deepEqual(calls,[['clearSqlHistory'],['setSqlHistoryEnabled',false]]);
  checkbox.checked=true; ctx.toggleHistoryEnabled(); ctx.saveToHistory('select 1');
  assert.deepEqual(JSON.parse(storage.history),['select 1']);
  assert.match(fs.readFileSync('src/Sidebar.html','utf8'),/id="historyEnabled"/);
});

test('Sidebar instance history feeds visible attach select with one-day TTL', () => {
  let now=1700000000000; const storage={}, options=[], input={value:''}, select={value:'inst-1',appendChild:x=>options.push(x)};
  const ctx = sidebarFunctions(['getClientInstanceIdError','saveInstanceToHistory','getInstanceHistoryList','persistInstanceHistoryList','renderInstanceHistory','selectAttachInstanceHistory'], {
    INSTANCE_HISTORY_KEY:'instances', INSTANCE_HISTORY_TTL_MS:86400000, MAX_HISTORY:10,window:{},t:x=>x,Date:{now:()=>now},
    localStorage:{getItem:k=>storage[k],setItem:(k,v)=>{storage[k]=v;}},
    document:{getElementById:id=>id==='attachInstanceSelect'?select:input,createElement:()=>({})}
  });
  ctx.saveInstanceToHistory('inst-1'); ctx.selectAttachInstanceHistory();
  assert.equal(input.value,'inst-1'); assert.ok(options.some(x=>x.value==='inst-1'));
  storage.instances=JSON.stringify([{instanceId:'fresh',savedAt:now-1},{instanceId:'old',savedAt:now-86400001},{instanceId:'../bad',savedAt:now},{instanceId:'future',savedAt:now+1}]);
  assert.equal(JSON.stringify(ctx.getInstanceHistoryList(now)),JSON.stringify([{instanceId:'fresh',savedAt:now-1}]));
});

test('Sidebar success summary escapes server-provided fields before rendering HTML', () => {
  const el={style:{},innerHTML:''};
  const ctx=sidebarFunctions(['escapeHtml','escapeAttr','formatMs','updateExecStatusDom'],{document:{getElementById:()=>el},t:x=>x});
  ctx.updateExecStatusDom({status:'success',summary:{rowCount:'<img src=x onerror=alert(1)>',executionTimeMs:1234}});
  assert.doesNotMatch(el.innerHTML,/<img/); assert.match(el.innerHTML,/&lt;img/);
  ctx.updateExecStatusDom({status:'submitted',instanceId:'inst"><img src=x>',logviewUrl:'https://example.com/?q="><script>bad</script>'});
  assert.doesNotMatch(el.innerHTML,/<img|<script/); assert.match(el.innerHTML,/&quot;&gt;&lt;script&gt;/);
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


test('Sidebar ignores catalog failure for a table no longer selected', () => {
  let fail, rendered = 0;
  const ctx = sidebarFunctions(['loadTableDetailForPreview'], {
    catalogDetailsLoading:Object.create(null), catalogRequestVersion:0,
    selectedCatalogTable:{schema:'s',table:'other'},
    document:{getElementById(){rendered++;return {};}},
    google:{script:{run:{withSuccessHandler(){return this;},withFailureHandler(fn){fail=fn;return this;},getTableDetail(){}}}}
  });
  ctx.loadTableDetailForPreview('s','first'); fail({message:'old failure'});
  assert.equal(rendered,0); assert.equal(ctx.catalogDetailsLoading['s.first'],undefined);
});

test('Sidebar stale history response cannot undo a newer privacy preference', () => {
  let success, applied=0;
  const run={withSuccessHandler(fn){success=fn;return this;},withFailureHandler(){return this;},getQueryHistory(){}};
  const ctx=sidebarFunctions(['syncHistoryFromBackend'], {window:{google:true},google:{script:{run}},
    historyRequestVersion:0,applyBackendHistory_(){applied++;}});
  ctx.syncHistoryFromBackend(); ctx.historyRequestVersion++;
  success({enabled:true,sqlItems:['old sql']}); assert.equal(applied,0);
});

test('Sidebar disabled remote history clears cached SQL without restoring remote SQL', () => {
  let rendered, removed=false;
  const ctx=sidebarFunctions(['applyBackendHistory_'], {getRawLocalSqlHistory_:()=>['secret'],isHistoryEnabled:()=>true,
    setHistoryEnabled(){},applyHistoryPreference(){},HISTORY_KEY:'history',
    localStorage:{removeItem(){removed=true;},setItem(){throw new Error('must not restore SQL');}},
    renderHistory:list=>{rendered=list;}});
  ctx.applyBackendHistory_({enabled:false,sqlItems:['remote secret']});
  assert.equal(removed,true); assert.equal(rendered.length,0);
});
