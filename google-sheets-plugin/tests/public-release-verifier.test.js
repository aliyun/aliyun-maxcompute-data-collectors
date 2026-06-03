const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const { verifyPublicRelease } = require('../scripts/verify-public-release');

const TEMP_ROOT = path.resolve('dist/public-release-verifier-test');
const SCOPE_CURRENT_ONLY = 'https://www.googleapis.com/auth/spreadsheets.currentonly';
const SCOPE_UI = 'https://www.googleapis.com/auth/script.container.ui';
const SCOPE_FETCH = 'https://www.googleapis.com/auth/script.external_request';
const SCOPE_STORAGE = 'https://www.googleapis.com/auth/script.storage';
const SCOPE_EMAIL = 'https://www.googleapis.com/auth/userinfo.email';

function writeFixture(files) {
  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
  fs.mkdirSync(path.join(TEMP_ROOT, 'docs'), { recursive: true });
  fs.mkdirSync(path.join(TEMP_ROOT, 'src'), { recursive: true });

  for (const [relativePath, content] of Object.entries(files)) {
    const absolutePath = path.join(TEMP_ROOT, relativePath);
    fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
    fs.writeFileSync(absolutePath, content);
  }

  return {
    rootDir: TEMP_ROOT,
    evidencePath: path.join(TEMP_ROOT, 'docs', 'external-qa-evidence-template.md'),
    manifestPath: path.join(TEMP_ROOT, 'src', 'appsscript.json')
  };
}

function manifestFixture() {
  return JSON.stringify({
    oauthScopes: [
      SCOPE_CURRENT_ONLY,
      SCOPE_UI,
      SCOPE_FETCH,
      SCOPE_STORAGE,
      SCOPE_EMAIL
    ]
  });
}

function filledEvidenceFixture() {
  return `# External QA Evidence

## Release Candidate

| Field | Value |
|-------|-------|
| Candidate version | 2026.05.14-rc1 |
| Apps Script project ID | script-project-123 |
| Standard Google Cloud project ID | cloud-project-123 |
| Apps Script version number | 42 |
| Editor add-on test deployment ID | AKfycbxEditorAddonDeployment |
| QA spreadsheet URL / ID | https://docs.google.com/spreadsheets/d/qa-sheet |
| QA MaxCompute project | qa_project |
| Tester Google account(s) | qa@example.com |
| Test date | 2026-05-14 |

## Local Repository Gates

| Gate | Evidence |
|------|----------|
| npm run release:local | 133 passed, 0 failed; json ok; 2026-05-14T10:00:00Z |
| src/appsscript.json / src/.clasp.example.json / dist/apps-script/appsscript.json / dist/apps-script/.clasp.json / package.json JSON parse | json ok |
| Production package excludes QA functions (dist/apps-script/Test.js absent) | excluded: Test.js |
| Git commit / tag under test | abc1234 |

## Apps Script Deployment Gates

| Gate | Expected Evidence | Result |
|------|-------------------|--------|
| Apps Script is attached to a standard Google Cloud project | Cloud project number is set in Apps Script project settings | Pass |
| OAuth consent app exists for the same Cloud project | Consent screen app name, support email, privacy URL, terms URL configured | Pass |
| OAuth scopes match src/appsscript.json | ${SCOPE_CURRENT_ONLY}; ${SCOPE_UI}; ${SCOPE_FETCH}; ${SCOPE_STORAGE}; ${SCOPE_EMAIL} | Pass |
| Apps Script version created | Immutable version number recorded above | Pass |
| Test deployment created as **Editor add-on** | Test deployment ID recorded above | Pass |
| QA spreadsheet install succeeds | Add-on available from Extensions / test deployment flow | Pass |
| MaxCompute menu appears | Screenshot archived | Pass |
| Settings sidebar opens | Screenshot archived | Pass |
| Query sidebar opens | Screenshot archived | Pass |

## Apps Script QA Functions

| Function | Result | Evidence |
|----------|--------|----------|
| runReleaseSmokeTests() | Pass | failed: 0 |
| test_connectionStatus() | Pass | log archived |
| test_readOnlySqlGuard() | Pass | log archived |
| test_endpointValidation() | Pass | log archived |
| test_listSchemas() | Pass | log archived |
| test_listTables() | Pass | log archived |
| test_getTableSchema() | Pass | log archived |
| test_listPartitions() | N/A | no partition table in QA project |
| test_executeSimpleQuery() | Pass | log archived |
| test_odpsSignature() | Pass | log archived |

## Google Sheets UI Smoke Tests

| Scenario | Expected Result | Result |
|----------|-----------------|--------|
| Save settings with QA AK/SK/project/endpoint | Success message | Pass |
| Test connection from settings sidebar | Success message | Pass |
| Run SELECT 1 AS id; from query sidebar | Result sheet created | Pass |
| Run a query returning no tabular result if available | Status row written | N/A |
| Run DROP TABLE t; | Rejected before submission | Pass |
| Run SHOW CREATE TABLE <qa_table>; if available | Allowed | Pass |
| Run a long query, then Cancel | Cancel request sent | Pass |
| Attach to completed Instance ID | Result fetched | Pass |
| Browse catalog schema/table/columns | Catalog expands | Pass |
| Load partitions for partitioned table | Partitions render | N/A |
| Switch language | Labels update | Pass |

## MaxCompute Audit Evidence

| Field | Expected | Evidence |
|-------|----------|----------|
| EXT_PLATFORM_ID | Gsheet | Verified |
| EXT_NODE_ONDUTY | Submitting Google account email | Verified |
| EXT_NODE_NAME | Google Spreadsheet name if available | Verified |
| EXT_NODE_ID | Google Spreadsheet ID | Verified |
| EXT_TASK_ID | Target Sheet name | Verified |
| Submitted SQL | User SQL is read-only and audit EXT fields are in task settings | Verified |
| Logview URL | Opens the matching Instance ID | Verified |

## Marketplace / OAuth Gates

| Gate | Expected Evidence | Result |
|------|-------------------|--------|
| Marketplace SDK app configuration points to the Editor add-on Apps Script version | Version number 42 | Pass |
| App visibility is set for internal/domain-limited test before public release | Test users can install | Pass |
| Required listing fields are complete | App name, short description, detailed description, category, support email, website | Pass |
| Listing assets uploaded | Icons and screenshots match the current UI | Pass |
| Privacy policy URL works | Public URL opens | Pass https://example.com/privacy |
| Terms of service URL works | Public URL opens | Pass https://example.com/terms |
| Support URL works | Public URL opens | Pass https://example.com/support |
| Data access / security disclosure completed | Matches docs/marketplace-submission-draft.md | Pass |
| OAuth verification submitted/approved if required | Verification status recorded | approved |
| Marketplace review submitted/approved for public release | Review status recorded | approved |

## Release Decision

| Question | Answer |
|----------|--------|
| Are all repository gates green? | Yes |
| Are all Apps Script QA gates green or explicitly N/A? | Yes |
| Are all real MaxCompute smoke tests green or explicitly N/A? | Yes |
| Are OAuth and Marketplace gates complete for the target visibility? | Yes |
| Public release approved by owner? | Yes |

Final decision:

- [ ] Hold release
- [ ] Internal/domain-limited release only
- [x] Public Marketplace release

Notes:

\`\`\`text
Release approved by owner.
\`\`\`
`;
}

test('public release verifier rejects the current TODO evidence template', () => {
  const result = verifyPublicRelease({
    evidencePath: path.resolve('docs/external-qa-evidence-template.md'),
    manifestPath: path.resolve('src/appsscript.json')
  });

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /TODO placeholders/);
  assert.match(result.failures.join('\n'), /Public Marketplace release/);
});

test('public release verifier accepts filled external release evidence', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture(),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.deepEqual(result, {
    ok: true,
    failures: [],
    evidencePath: fixture.evidencePath,
    manifestPath: fixture.manifestPath
  });

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier requires all manifest OAuth scopes in external evidence', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture().replace(SCOPE_STORAGE, 'missing-storage-scope'),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), new RegExp(SCOPE_STORAGE.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier rejects conflicting final release decisions', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture().replace('- [ ] Hold release', '- [x] Hold release'),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Hold release/);

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier rejects non-final local release references', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture().replace(
      '| Git commit / tag under test | abc1234 |',
      '| Git commit / tag under test | abc1234 plus current uncommitted workspace changes; replace before deployment |'
    ),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /concrete clean release commit or tag/);

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier rejects incomplete Apps Script QA rows', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture().replace(
      '| test_listTables() | Pass | log archived |',
      '| test_listTables() | Failed | schema listing failed |'
    ),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /test_listTables\(\).*passing or N\/A result/);

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier rejects missing UI and audit evidence', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture()
      .replace('| Browse catalog schema/table/columns | Catalog expands | Pass |', '| Browse catalog schema/table/columns | Catalog expands | Failed |')
      .replace('| EXT_PLATFORM_ID | Gsheet | Verified |', '| EXT_PLATFORM_ID | Gsheet | Missing |'),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Browse catalog schema\/table\/columns/);
  assert.match(result.failures.join('\n'), /EXT_PLATFORM_ID/);

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});

test('public release verifier rejects incomplete Marketplace gate rows', () => {
  const fixture = writeFixture({
    'docs/external-qa-evidence-template.md': filledEvidenceFixture().replace(
      '| Listing assets uploaded | Icons and screenshots match the current UI | Pass |',
      '| Listing assets uploaded | Icons and screenshots match the current UI | Pending screenshots |'
    ),
    'src/appsscript.json': manifestFixture()
  });

  const result = verifyPublicRelease(fixture);

  assert.equal(result.ok, false);
  assert.match(result.failures.join('\n'), /Listing assets uploaded/);

  fs.rmSync(TEMP_ROOT, { recursive: true, force: true });
});
