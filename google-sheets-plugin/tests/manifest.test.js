const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const { buildRelease } = require('../scripts/build-release');

const REQUIRED_SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/script.container.ui',
  'https://www.googleapis.com/auth/script.external_request',
  'https://www.googleapis.com/auth/script.storage',
  'https://www.googleapis.com/auth/script.scriptapp',
  'https://www.googleapis.com/auth/userinfo.email'
];

const REQUIRED_ENDPOINT_PREFIXES = [
  'https://service.cn-hangzhou.maxcompute.aliyun.com/',
  'https://service.cn-shanghai.maxcompute.aliyun.com/',
  'https://service.cn-beijing.maxcompute.aliyun.com/',
  'https://service.cn-shenzhen.maxcompute.aliyun.com/',
  'https://service.cn-hongkong.maxcompute.aliyun.com/',
  'https://service.ap-southeast-1.maxcompute.aliyun.com/',
  'https://service.ap-northeast-1.maxcompute.aliyun.com/',
  'https://service.ap-northeast-2.maxcompute.aliyun.com/',
  'https://service.eu-central-1.maxcompute.aliyun.com/',
  'https://service.eu-west-1.maxcompute.aliyun.com/',
  'https://service.us-west-1.maxcompute.aliyun.com/',
  'https://service.us-east-1.maxcompute.aliyun.com/',
  'https://service.me-east-1.maxcompute.aliyun.com/',
  'https://service.me-central-1.maxcompute.aliyun.com/'
];

function readManifest() {
  return JSON.parse(fs.readFileSync('src/appsscript.json', 'utf8'));
}

function readClaspConfig() {
  const configPath = fs.existsSync('src/.clasp.json')
    ? 'src/.clasp.json'
    : 'src/.clasp.example.json';
  return JSON.parse(fs.readFileSync(configPath, 'utf8'));
}

test('Apps Script manifest uses V8 and Stackdriver exception logging', () => {
  const manifest = readManifest();
  assert.equal(manifest.runtimeVersion, 'V8');
  assert.equal(manifest.exceptionLogging, 'STACKDRIVER');
});

test('Apps Script manifest includes required editor add-on scopes only once', () => {
  const manifest = readManifest();
  const scopes = manifest.oauthScopes || [];
  for (const scope of REQUIRED_SCOPES) {
    assert.ok(scopes.includes(scope), `missing scope: ${scope}`);
    assert.equal(scopes.filter((candidate) => candidate === scope).length, 1, `duplicate scope: ${scope}`);
  }

  assert.deepEqual(scopes.slice().sort(), REQUIRED_SCOPES.slice().sort());
  assert.equal(scopes.includes('https://www.googleapis.com/auth/drive'), false);
});

test('Apps Script manifest whitelists MaxCompute endpoint prefixes', () => {
  const manifest = readManifest();
  const whitelist = manifest.urlFetchWhitelist || [];
  for (const endpoint of REQUIRED_ENDPOINT_PREFIXES) {
    assert.ok(whitelist.includes(endpoint), `missing endpoint whitelist: ${endpoint}`);
  }

  for (const endpoint of whitelist) {
    assert.match(endpoint, /^(?:https:\/\/service\.[a-z0-9-]+\.maxcompute\.aliyun\.com\/|https:\/\/www\.googleapis\.com\/|https:\/\/\*\.oss-[a-z0-9-]+\.aliyuncs\.com\/)$/);
  }

  assert.ok(whitelist.includes('https://www.googleapis.com/'), 'missing Google userinfo whitelist');
});

test('clasp config pushes Apps Script source, HTML sidebars, and manifest JSON', () => {
  const config = readClaspConfig();
  const scriptExtensions = config.scriptExtensions || [];
  const htmlExtensions = config.htmlExtensions || [];
  const jsonExtensions = config.jsonExtensions || [];

  assert.equal(config.rootDir, '');
  assert.ok(scriptExtensions.includes('.js'), 'missing .js in scriptExtensions');
  assert.ok(scriptExtensions.includes('.gs'), 'missing .gs in scriptExtensions');
  assert.equal(scriptExtensions.includes('.html'), false, '.html belongs in htmlExtensions');
  assert.equal(scriptExtensions.includes('.json'), false, '.json belongs in jsonExtensions');
  assert.ok(htmlExtensions.includes('.html'), 'missing .html in htmlExtensions');
  assert.ok(jsonExtensions.includes('.json'), 'missing .json in jsonExtensions');
});

test('tracked clasp example does not publish a real Apps Script script id', () => {
  const config = JSON.parse(fs.readFileSync('src/.clasp.example.json', 'utf8'));

  assert.equal(config.scriptId, '');
  assert.equal(config.rootDir, '');
});

test('clasp config classifies required source files with expected Apps Script types', () => {
  const config = readClaspConfig();
  const extensionMap = {
    SERVER_JS: config.scriptExtensions || [],
    HTML: config.htmlExtensions || [],
    JSON: config.jsonExtensions || []
  };
  const expectedTypes = {
    'Code.js': 'SERVER_JS',
    'Config.js': 'SERVER_JS',
    'OdpsSigner.js': 'SERVER_JS',
    'SettingsParser.js': 'SERVER_JS',
    'SqlExecutor.js': 'SERVER_JS',
    'TableBrowser.js': 'SERVER_JS',
    'Test.js': 'SERVER_JS',
    'Sidebar.html': 'HTML',
    'Settings.html': 'HTML',
    'appsscript.json': 'JSON'
  };

  for (const [file, expectedType] of Object.entries(expectedTypes)) {
    const fullPath = path.join('src', file);
    assert.equal(fs.existsSync(fullPath), true, `missing required Apps Script file: ${file}`);
    assert.equal(classifyClaspFile(file, extensionMap), expectedType, `${file} classified incorrectly`);
  }
});

test('release clasp config is ready for pushing production package', () => {
  const outputDir = path.resolve('dist/apps-script-manifest-test');
  buildRelease({ outputDir });

  const config = JSON.parse(fs.readFileSync(path.join(outputDir, '.clasp.json'), 'utf8'));
  const releaseFiles = fs.readdirSync(outputDir);

  assert.equal(config.rootDir, '');
  assert.equal(releaseFiles.includes('Test.js'), false);
  assert.ok(releaseFiles.includes('Code.js'), 'release package missing Code.js');
  assert.ok(releaseFiles.includes('Sidebar.html'), 'release package missing Sidebar.html');
  assert.ok(releaseFiles.includes('Settings.html'), 'release package missing Settings.html');
  assert.ok(releaseFiles.includes('appsscript.json'), 'release package missing appsscript.json');

  fs.rmSync(outputDir, { recursive: true, force: true });
});

function classifyClaspFile(file, extensionMap) {
  const ext = path.extname(file).toLowerCase();
  for (const [type, extensions] of Object.entries(extensionMap)) {
    if (extensions.map((candidate) => candidate.toLowerCase()).includes(ext)) {
      return type;
    }
  }
  return '';
}
