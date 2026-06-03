const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const { buildRelease } = require('../scripts/build-release');
const { verifyReleasePackage } = require('../scripts/verify-release-package');

const TEMP_DIR = path.resolve('dist/release-package-verifier-test');

function cleanTemp() {
  fs.rmSync(TEMP_DIR, { recursive: true, force: true });
}

function buildFixturePackage() {
  cleanTemp();
  buildRelease({ outputDir: TEMP_DIR });
  return TEMP_DIR;
}

test('release package verifier accepts the built production package', () => {
  const packageDir = buildFixturePackage();

  const result = verifyReleasePackage({ packageDir });

  assert.deepEqual(result, {
    ok: true,
    failures: [],
    packageDir
  });

  cleanTemp();
});

test('release package verifier rejects QA-only files in the production package', () => {
  const packageDir = buildFixturePackage();
  fs.copyFileSync(path.resolve('src/Test.js'), path.join(packageDir, 'Test.js'));

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Test\.js/);

  cleanTemp();
});

test('release package verifier rejects broad OAuth scopes', () => {
  const packageDir = buildFixturePackage();
  const manifestPath = path.join(packageDir, 'appsscript.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  manifest.oauthScopes.push('https://www.googleapis.com/auth/drive');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Forbidden broad OAuth scope/);

  cleanTemp();
});

test('release package verifier rejects unknown OAuth scopes', () => {
  const packageDir = buildFixturePackage();
  const manifestPath = path.join(packageDir, 'appsscript.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  manifest.oauthScopes.push('https://www.googleapis.com/auth/script.send_mail');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Unexpected OAuth scope/);

  cleanTemp();
});

test('release package verifier rejects missing required MaxCompute whitelist entries', () => {
  const packageDir = buildFixturePackage();
  const manifestPath = path.join(packageDir, 'appsscript.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  manifest.urlFetchWhitelist = manifest.urlFetchWhitelist.filter(
    (endpoint) => endpoint !== 'https://service.ap-southeast-1.maxcompute.aliyun.com/'
  );
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Required URL fetch whitelist entry.*ap-southeast-1/);

  cleanTemp();
});

test('release package verifier rejects duplicate required MaxCompute whitelist entries', () => {
  const packageDir = buildFixturePackage();
  const manifestPath = path.join(packageDir, 'appsscript.json');
  const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  manifest.urlFetchWhitelist.push('https://service.ap-southeast-1.maxcompute.aliyun.com/');
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Required URL fetch whitelist entry.*ap-southeast-1/);

  cleanTemp();
});

test('release package verifier rejects unexpected public Apps Script callables', () => {
  const packageDir = buildFixturePackage();
  fs.appendFileSync(path.join(packageDir, 'Code.js'), '\nfunction accidentalPublicCallable() { return true; }\n');

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /accidentalPublicCallable/);

  cleanTemp();
});

test('release package verifier rejects forbidden production Apps Script APIs', () => {
  const packageDir = buildFixturePackage();
  fs.appendFileSync(path.join(packageDir, 'Code.js'), '\nfunction onOpen() { DriveApp.getFiles(); }\n');

  const result = verifyReleasePackage({ packageDir });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Forbidden Apps Script API.*DriveApp/);

  cleanTemp();
});
