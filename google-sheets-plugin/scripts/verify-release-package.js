const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_PACKAGE_DIR = path.resolve(__dirname, '..', 'dist', 'apps-script');

const REQUIRED_RELEASE_FILES = [
  '.clasp.json',
  'Code.js',
  'Config.js',
  'OdpsSigner.js',
  'OssExporter.js',
  'OssSigner.js',
  'Scheduler.js',
  'SettingsParser.js',
  'Settings.html',
  'Sidebar.html',
  'SqlExecutor.js',
  'TableBrowser.js',
  'appsscript.json'
];

const FORBIDDEN_RELEASE_FILES = [
  'Test.js'
];

const REQUIRED_SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/script.container.ui',
  'https://www.googleapis.com/auth/script.external_request',
  'https://www.googleapis.com/auth/script.storage',
  'https://www.googleapis.com/auth/script.scriptapp',
  'https://www.googleapis.com/auth/userinfo.email'
];

const FORBIDDEN_SCOPES = [
  'https://www.googleapis.com/auth/drive'
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

const REQUIRED_NON_MAXCOMPUTE_URLS = [
  'https://www.googleapis.com/',
  'https://*.oss-ap-northeast-1.aliyuncs.com/',
  'https://*.oss-ap-northeast-2.aliyuncs.com/',
  'https://*.oss-ap-southeast-1.aliyuncs.com/',
  'https://*.oss-ap-southeast-3.aliyuncs.com/',
  'https://*.oss-ap-southeast-5.aliyuncs.com/',
  'https://*.oss-cn-beijing.aliyuncs.com/',
  'https://*.oss-cn-chengdu.aliyuncs.com/',
  'https://*.oss-cn-hangzhou.aliyuncs.com/',
  'https://*.oss-cn-hongkong.aliyuncs.com/',
  'https://*.oss-cn-shanghai.aliyuncs.com/',
  'https://*.oss-cn-shenzhen.aliyuncs.com/',
  'https://*.oss-cn-wulanchabu.aliyuncs.com/',
  'https://*.oss-cn-zhangjiakou.aliyuncs.com/',
  'https://*.oss-eu-central-1.aliyuncs.com/',
  'https://*.oss-eu-west-1.aliyuncs.com/',
  'https://*.oss-me-central-1.aliyuncs.com/',
  'https://*.oss-me-east-1.aliyuncs.com/',
  'https://*.oss-na-south-1.aliyuncs.com/',
  'https://*.oss-us-east-1.aliyuncs.com/',
  'https://*.oss-us-west-1.aliyuncs.com/'
];

const FORBIDDEN_PRODUCTION_APIS = [
  'DriveApp',
  'GmailApp',
  'MailApp',
  'DocumentApp',
  'SlidesApp',
  'CalendarApp',
  'ContactsApp',
  'GroupsApp',
  'AdminDirectory',
  'OAuth2'
];

const ALLOWED_PUBLIC_FUNCTIONS = [
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
];

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function listFiles(packageDir) {
  if (!fs.existsSync(packageDir)) {
    throw new Error(`Release package directory is missing: ${path.relative(process.cwd(), packageDir)}`);
  }
  return fs.readdirSync(packageDir).sort();
}

function verifyReleasePackage(options = {}) {
  const packageDir = options.packageDir || DEFAULT_PACKAGE_DIR;
  const failures = [];

  let files;
  try {
    files = listFiles(packageDir);
  } catch (e) {
    return {
      ok: false,
      failures: [e.message],
      packageDir
    };
  }

  const expectedFiles = REQUIRED_RELEASE_FILES.slice().sort();
  if (JSON.stringify(files) !== JSON.stringify(expectedFiles)) {
    failures.push(`Release package file list mismatch. expected=${expectedFiles.join(', ')} actual=${files.join(', ')}`);
  }

  for (const file of FORBIDDEN_RELEASE_FILES) {
    if (files.includes(file)) {
      failures.push(`Forbidden QA-only file is present in release package: ${file}`);
    }
  }

  let manifest = null;
  let clasp = null;
  try {
    manifest = readJson(path.join(packageDir, 'appsscript.json'));
  } catch (e) {
    failures.push(`Invalid appsscript.json: ${e.message}`);
  }
  try {
    clasp = readJson(path.join(packageDir, '.clasp.json'));
  } catch (e) {
    failures.push(`Invalid .clasp.json: ${e.message}`);
  }

  if (manifest) {
    if (manifest.runtimeVersion !== 'V8') {
      failures.push('Apps Script runtimeVersion must be V8.');
    }
    if (manifest.exceptionLogging !== 'STACKDRIVER') {
      failures.push('Apps Script exceptionLogging must be STACKDRIVER.');
    }

    const scopes = manifest.oauthScopes || [];
    for (const scope of REQUIRED_SCOPES) {
      const count = scopes.filter((candidate) => candidate === scope).length;
      if (count !== 1) {
        failures.push(`Required OAuth scope must appear exactly once: ${scope}`);
      }
    }
    for (const scope of FORBIDDEN_SCOPES) {
      if (scopes.includes(scope)) {
        failures.push(`Forbidden broad OAuth scope is present: ${scope}`);
      }
    }
    for (const scope of scopes) {
      if (!REQUIRED_SCOPES.includes(scope)) {
        failures.push(`Unexpected OAuth scope is present: ${scope}`);
      }
    }

    const whitelist = manifest.urlFetchWhitelist || [];
    for (const endpoint of REQUIRED_ENDPOINT_PREFIXES) {
      const count = whitelist.filter((candidate) => candidate === endpoint).length;
      if (count !== 1) {
        failures.push(`Required URL fetch whitelist entry must appear exactly once: ${endpoint}`);
      }
    }
    for (const endpoint of REQUIRED_NON_MAXCOMPUTE_URLS) {
      const count = whitelist.filter((candidate) => candidate === endpoint).length;
      if (count !== 1) {
        failures.push(`Required URL fetch whitelist entry must appear exactly once: ${endpoint}`);
      }
    }
    for (const endpoint of whitelist) {
      if (!/^https:\/\/service\.[a-z0-9-]+\.maxcompute\.aliyun\.com\/$/.test(endpoint) &&
          !REQUIRED_NON_MAXCOMPUTE_URLS.includes(endpoint)) {
        failures.push(`Unexpected URL fetch whitelist entry: ${endpoint}`);
      }
    }
  }

  if (clasp) {
    if (clasp.rootDir !== '') {
      failures.push('Release .clasp.json rootDir must be empty when pushing from dist/apps-script.');
    }
    if (!Array.isArray(clasp.scriptExtensions) || !clasp.scriptExtensions.includes('.js')) {
      failures.push('Release .clasp.json must classify .js files as script files.');
    }
    if (!Array.isArray(clasp.htmlExtensions) || !clasp.htmlExtensions.includes('.html')) {
      failures.push('Release .clasp.json must classify .html files as HTML files.');
    }
    if (!Array.isArray(clasp.jsonExtensions) || !clasp.jsonExtensions.includes('.json')) {
      failures.push('Release .clasp.json must classify .json files as JSON files.');
    }
  }

  const productionSource = files
    .filter((file) => file.endsWith('.js'))
    .map((file) => fs.readFileSync(path.join(packageDir, file), 'utf8'))
    .join('\n');
  const publicFunctions = [...productionSource.matchAll(/^function\s+([A-Za-z0-9_]+)\s*\(/gm)]
    .map((match) => match[1])
    .filter((name) => !name.endsWith('_'))
    .sort();
  const allowedPublicFunctions = ALLOWED_PUBLIC_FUNCTIONS.slice().sort();
  if (JSON.stringify(publicFunctions) !== JSON.stringify(allowedPublicFunctions)) {
    failures.push(`Public Apps Script callable surface mismatch. expected=${allowedPublicFunctions.join(', ')} actual=${publicFunctions.join(', ')}`);
  }

  if (/function\s+runReleaseSmokeTests\s*\(/.test(productionSource) ||
      /function\s+test_[A-Za-z0-9_]+\s*\(/.test(productionSource)) {
    failures.push('QA smoke/test functions must not be present in the production package.');
  }

  for (const apiName of FORBIDDEN_PRODUCTION_APIS) {
    const pattern = new RegExp(`\\b${apiName}\\b`);
    if (pattern.test(productionSource)) {
      failures.push(`Forbidden Apps Script API is present in production package: ${apiName}`);
    }
  }

  return {
    ok: failures.length === 0,
    failures,
    packageDir
  };
}

if (require.main === module) {
  const result = verifyReleasePackage();
  if (!result.ok) {
    console.error('Release package verification failed:');
    for (const failure of result.failures) {
      console.error(`- ${failure}`);
    }
    process.exitCode = 1;
  } else {
    console.log('Release package verification passed.');
  }
}

module.exports = {
  verifyReleasePackage,
  ALLOWED_PUBLIC_FUNCTIONS
};
