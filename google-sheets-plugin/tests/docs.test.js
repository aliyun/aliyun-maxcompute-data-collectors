const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');

function read(path) {
  return fs.readFileSync(path, 'utf8');
}

function compactWhitespace(text) {
  return text.replace(/\s+/g, ' ');
}

test('release checklist tracks external Marketplace and OAuth gates', () => {
  const doc = compactWhitespace(read('docs/release-checklist.md'));

  assert.match(doc, /standard Google Cloud project/);
  assert.match(doc, /immutable Apps Script version/);
  assert.match(doc, /Marketplace SDK listing points to that exact Apps Script version number/);
  assert.match(doc, /deployment IDs are for CardService-based Google Workspace add-ons/);
  assert.match(doc, /OAuth consent screen/);
  assert.match(doc, /Requested OAuth scopes match in all three places/);
  assert.match(doc, /no broad Drive\/full-spreadsheet scopes/);
  assert.match(doc, /https:\/\/www\.googleapis\.com\/auth\/userinfo\.email/);
  assert.match(doc, /OAuth verification/);
  assert.match(doc, /Marketplace app review/);
  assert.match(doc, /npm run release:local/);
  assert.match(doc, /npm run release:verify-public/);
  assert.match(doc, /external-qa-evidence-template\.md/);
});

test('release checklist links official Google external gate references', () => {
  const doc = read('docs/release-checklist.md');

  assert.match(doc, /Official Google References/);
  assert.match(doc, /publish-add-on-overview/);
  assert.match(doc, /testing-editor-addons/);
  assert.match(doc, /configure-oauth-consent-screen/);
  assert.match(doc, /enable-configure-sdk/);
  assert.match(doc, /about-app-review/);
  assert.match(doc, /Apps Script version number for an Editor add-on/);
  assert.match(doc, /OAuth scopes aligned across the OAuth consent screen, Marketplace SDK,\n  and Apps Script manifest/);
});

test('external QA evidence template covers real release gates', () => {
  const doc = read('docs/external-qa-evidence-template.md');

  assert.match(doc, /Apps Script version number/);
  assert.match(doc, /Editor add-on test deployment ID/);
  assert.match(doc, /do not use a Google Workspace add-on deployment ID/);
  assert.match(doc, /QA spreadsheet/);
  assert.match(doc, /QA MaxCompute project/);
  assert.match(doc, /Apps Script QA Functions/);
  assert.match(doc, /Google Sheets UI Smoke Tests/);
  assert.match(doc, /MaxCompute Audit Evidence/);
  assert.match(doc, /Marketplace \/ OAuth Gates/);
  assert.match(doc, /npm run release:local/);
  assert.match(doc, /Public Marketplace release/);
});

test('marketplace draft warns to align manifest OAuth and Marketplace scopes', () => {
  const doc = compactWhitespace(read('docs/marketplace-submission-draft.md'));

  assert.match(doc, /Editor add-on for Google Sheets/);
  assert.match(doc, /Apps Script \*\*version number\*\*/);
  assert.match(doc, /Google Workspace add-on deployment ID/);
  assert.match(doc, /standard Google Cloud project/);
  assert.match(doc, /src\/appsscript\.json/);
  assert.match(doc, /OAuth consent screen \/ data access configuration/);
  assert.match(doc, /Google Workspace Marketplace SDK app configuration/);
  assert.match(doc, /`EXT_NODE_ONDUTY` records the Google account email that submitted/);
});

test('completion audit separates repository readiness from external gates', () => {
  const doc = read('docs/completion-audit.md');

  assert.match(doc, /Concrete Success Criteria/);
  assert.match(doc, /Prompt-To-Artifact Checklist/);
  assert.match(doc, /Latest Local Evidence/);
  assert.match(doc, /Remaining Required Evidence/);
  assert.match(doc, /not complete for public Marketplace release/);
  assert.match(doc, /release:verify-public/);
  assert.match(doc, /external-qa-evidence-template\.md/);
});

test('long running job guide explains Instance ID attach workflow and limits', () => {
  const doc = read('docs/long-running-jobs-instance-attach.md');

  assert.match(doc, /Instance ID \+ Attach/);
  assert.match(doc, /约 6 分钟/);
  assert.match(doc, /Attach to existing job/);
  assert.match(doc, /Target Sheet/);
  assert.match(doc, /1 天/);
  assert.match(doc, /10000 行/);
  assert.match(doc, /不会重新执行 SQL/);
  assert.match(doc, /不会同步到其它浏览器或其它设备/);
});

test('repository license metadata is Apache 2.0', () => {
  const readme = read('README.md');
  const pkg = JSON.parse(read('package.json'));
  const license = read('LICENSE');

  assert.equal(pkg.license, 'Apache-2.0');
  assert.match(readme, /Apache License 2\.0/);
  assert.doesNotMatch(readme, new RegExp('MIT ' + 'License'));
  assert.match(license, /Apache License/);
  assert.match(license, /Version 2\.0, January 2004/);
});
