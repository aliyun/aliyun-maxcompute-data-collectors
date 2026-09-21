const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const { buildRelease } = require('../scripts/build-release');

const ALLOWED_PRODUCTION_PUBLIC_FUNCTIONS = new Set([
  'onInstall',
  'onOpen',
  'showSidebar',
  'clearCurrentSheet',
  'switchLanguageToEn',
  'switchLanguageToZh',
  'executeQuery',
  'submitQuery',
  'getQueryProgress',
  'writeQueryResult',
  'cancelQuery',
  'getSheetNames',
  'getConnectionStatus',
  'testConnection',
  'getSchemas',
  'getTables',
  'getTableDetail',
  'getPartitions',
  'getUserLanguage',
  'getQueryHistory',
  'appendSqlHistory',
  'removeSqlHistoryAt',
  'clearSqlHistory',
  'setSqlHistoryEnabled',
  'appendInstanceHistory',
  'buildLogviewUrl_',
  'getMcConfigForUi',
  'showSettings',
  'saveMcConfig',
  'testMcConnection',
  'activateSheet',
  'attachToInstance',
  'clearJobList',
  'deleteSchedule',
  'exportSheetToCsv',
  'getActiveSheetInfo',
  'getAllSheetSqlBindings',
  'getExportPreferences',
  'getExportableSheets',
  'getJobList',
  'getOssConfigForUi',
  'getOssExportStatus',
  'getScheduleList',
  'getScheduleTriggerStatus',
  'installScheduleTrigger',
  'loadSheetSql',
  'removeJobRecord',
  'saveExportPreferences',
  'saveJobRecord',
  'saveOssConfig',
  'saveSchedule',
  'saveSheetSql',
  'switchSheet',
  'testOssConnection',
  'toggleSchedule',
  'uninstallScheduleTrigger'
]);

test('release build excludes Apps Script QA functions from production package', () => {
  const outputDir = path.resolve('dist/apps-script-test');

  const result = buildRelease({ outputDir });

  assert.equal(fs.existsSync(path.join(outputDir, 'Code.js')), true);
  assert.equal(fs.existsSync(path.join(outputDir, 'Sidebar.html')), true);
  assert.equal(fs.existsSync(path.join(outputDir, 'Settings.html')), true);
  assert.equal(fs.existsSync(path.join(outputDir, 'appsscript.json')), true);
  assert.equal(fs.existsSync(path.join(outputDir, '.clasp.json')), true);
  assert.equal(fs.existsSync(path.join(outputDir, 'Test.js')), false);
  assert.equal(result.excluded.includes('Test.js'), true);

  const productionSource = fs.readdirSync(outputDir)
    .filter((file) => file.endsWith('.js'))
    .map((file) => fs.readFileSync(path.join(outputDir, file), 'utf8'))
    .join('\n');

  assert.doesNotMatch(productionSource, /function\s+runReleaseSmokeTests\s*\(/);
  assert.doesNotMatch(productionSource, /function\s+test_listSchemas\s*\(/);

  fs.rmSync(outputDir, { recursive: true, force: true });
});

test('release build copies only explicit production allowlist files', () => {
  const outputDir = path.resolve('dist/apps-script-allowlist-test');

  const result = buildRelease({ outputDir });
  const releaseFiles = fs.readdirSync(outputDir).sort();

  assert.deepEqual(releaseFiles, [
    '.clasp.json',
    'Code.js',
    'Config.js',
    'OdpsSigner.js',
    'OssExporter.js',
    'OssSigner.js',
    'Scheduler.js',
    'Settings.html',
    'SettingsParser.js',
    'Sidebar.html',
    'SqlExecutor.js',
    'TableBrowser.js',
    'appsscript.json'
  ]);
  assert.deepEqual(result.copied, releaseFiles);

  fs.rmSync(outputDir, { recursive: true, force: true });
});

test('release build fails if a required production file is missing', () => {
  const outputDir = path.resolve('dist/apps-script-missing-file-test');
  const sourceDir = path.resolve('dist/apps-script-missing-source-test');

  fs.rmSync(outputDir, { recursive: true, force: true });
  fs.rmSync(sourceDir, { recursive: true, force: true });
  fs.mkdirSync(sourceDir, { recursive: true });

  assert.throws(() => buildRelease({ sourceDir, outputDir }), /Required release source file is missing: \.clasp\.json or \.clasp\.example\.json/);

  fs.rmSync(outputDir, { recursive: true, force: true });
  fs.rmSync(sourceDir, { recursive: true, force: true });
});

test('release build exposes only allowlisted production Apps Script functions', () => {
  const outputDir = path.resolve('dist/apps-script-public-surface-test');
  buildRelease({ outputDir });

  const productionSource = fs.readdirSync(outputDir)
    .filter((file) => file.endsWith('.js'))
    .map((file) => fs.readFileSync(path.join(outputDir, file), 'utf8'))
    .join('\n');

  const publicFunctions = [...productionSource.matchAll(/^function\s+([A-Za-z0-9_]+)\s*\(/gm)]
    .map((match) => match[1])
    .filter((name) => !name.endsWith('_'))
    .sort();

  assert.deepEqual(publicFunctions, Array.from(ALLOWED_PRODUCTION_PUBLIC_FUNCTIONS)
    .filter((name) => !name.endsWith('_'))
    .sort());

  fs.rmSync(outputDir, { recursive: true, force: true });
});
