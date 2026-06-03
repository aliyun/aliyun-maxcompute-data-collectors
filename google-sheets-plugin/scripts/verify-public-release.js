const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_ROOT_DIR = path.resolve(__dirname, '..');
const DEFAULT_EVIDENCE_PATH = path.join(DEFAULT_ROOT_DIR, 'docs', 'external-qa-evidence-template.md');
const DEFAULT_MANIFEST_PATH = path.join(DEFAULT_ROOT_DIR, 'src', 'appsscript.json');

const REQUIRED_SECTIONS = [
  'Release Candidate',
  'Local Repository Gates',
  'Apps Script Deployment Gates',
  'Apps Script QA Functions',
  'Google Sheets UI Smoke Tests',
  'MaxCompute Audit Evidence',
  'Marketplace / OAuth Gates',
  'Release Decision'
];

function readText(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Required file is missing: ${path.relative(process.cwd(), filePath)}`);
  }
  return fs.readFileSync(filePath, 'utf8');
}

function readManifestScopes(manifestPath) {
  const manifest = JSON.parse(readText(manifestPath));
  if (!Array.isArray(manifest.oauthScopes) || manifest.oauthScopes.length === 0) {
    throw new Error('Apps Script manifest must define oauthScopes for public release verification.');
  }
  return manifest.oauthScopes;
}

function hasCheckedItem(text, label) {
  return new RegExp(`^- \\[[xX]\\] ${escapeRegExp(label)}\\s*$`, 'm').test(text);
}

function findLineContaining(text, label) {
  return text.split(/\r?\n/).find((line) => line.includes(label)) || '';
}

function normalizeTableCell(value) {
  return String(value || '')
    .replace(/`/g, '')
    .replace(/\*\*/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function getTableValue(text, field) {
  const cells = getTableRowCells(text, field);
  return cells.length ? cells[0] : '';
}

function getTableRowCells(text, firstCell) {
  const expected = normalizeTableCell(firstCell);
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('|') || !trimmed.endsWith('|')) continue;
    const cells = trimmed.slice(1, -1).split('|').map((cell) => cell.trim());
    if (cells.length > 1 && normalizeTableCell(cells[0]) === expected) {
      return cells.slice(1);
    }
  }
  return [];
}

function getLastTableCell(text, firstCell) {
  const cells = getTableRowCells(text, firstCell);
  return cells.length ? cells[cells.length - 1] : '';
}

function getTableCell(text, firstCell, index) {
  const cells = getTableRowCells(text, firstCell);
  if (!cells.length) return '';
  return cells[index] || '';
}

function isPassLike(value) {
  return /\b(pass(?:ed)?|success|green|verified|approved|yes|ok)\b/i.test(value);
}

function isPassOrNa(value) {
  return isPassLike(value) || /\b(?:n\/a|not applicable)\b/i.test(value);
}

function isConcreteReleaseRef(value) {
  if (!value || /\b(?:TODO|replace|uncommitted|workspace changes|dirty)\b/i.test(value)) {
    return false;
  }
  return /^[A-Za-z0-9._/@:-]{7,}$/.test(value);
}

function requireRowResult(failures, evidence, label, options = {}) {
  const result = typeof options.cellIndex === 'number'
    ? getTableCell(evidence, label, options.cellIndex)
    : getLastTableCell(evidence, label);
  if (!result) {
    failures.push(`Missing required evidence row: ${label}`);
    return;
  }
  const ok = options.allowNa ? isPassOrNa(result) : isPassLike(result);
  if (!ok) {
    failures.push(`${label} must have a passing${options.allowNa ? ' or N/A' : ''} result.`);
  }
}

function requireRows(failures, evidence, labels, options = {}) {
  for (const label of labels) {
    requireRowResult(failures, evidence, label, options);
  }
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function verifyPublicRelease(options = {}) {
  const rootDir = options.rootDir || DEFAULT_ROOT_DIR;
  const evidencePath = options.evidencePath || DEFAULT_EVIDENCE_PATH;
  const manifestPath = options.manifestPath || path.join(rootDir, 'src', 'appsscript.json');

  const evidence = readText(evidencePath);
  const scopes = readManifestScopes(manifestPath);
  const failures = [];

  for (const section of REQUIRED_SECTIONS) {
    if (!new RegExp(`^## ${escapeRegExp(section)}\\s*$`, 'm').test(evidence)) {
      failures.push(`Missing required evidence section: ${section}`);
    }
  }

  if (/\bTODO\b/i.test(evidence)) {
    failures.push('External evidence still contains TODO placeholders.');
  }
  if (/\[PLACEHOLDER[^\]]*\]/i.test(evidence)) {
    failures.push('External evidence still contains PLACEHOLDER values.');
  }

  const appsScriptVersion = getTableValue(evidence, 'Apps Script version number');
  if (!/\d+/.test(appsScriptVersion)) {
    failures.push('Apps Script version number must be recorded as a concrete immutable version.');
  }

  const testDeploymentId = getTableValue(evidence, 'Editor add-on test deployment ID');
  if (!testDeploymentId || /TODO|deployment ID/i.test(testDeploymentId)) {
    failures.push('Editor add-on test deployment ID must be recorded.');
  }

  if (!/\b\d+\s+passed,\s+0\s+failed\b/.test(evidence)) {
    failures.push('Local evidence must include a release:local pass count in the form "N passed, 0 failed".');
  }
  if (!/\bjson ok\b/.test(evidence)) {
    failures.push('Local evidence must include the release JSON parse result: "json ok".');
  }
  if (!/\bexcluded:\s*Test\.js\b/.test(evidence)) {
    failures.push('Local evidence must confirm the production package excludes Test.js.');
  }

  const releaseRef = getTableValue(evidence, 'Git commit / tag under test');
  if (!isConcreteReleaseRef(releaseRef)) {
    failures.push('Git commit / tag under test must be a concrete clean release commit or tag.');
  }

  for (const scope of scopes) {
    if (!evidence.includes(scope)) {
      failures.push(`OAuth scope missing from external evidence: ${scope}`);
    }
  }

  requireRows(failures, evidence, [
    'Apps Script is attached to a standard Google Cloud project',
    'OAuth consent app exists for the same Cloud project',
    'OAuth scopes match `src/appsscript.json`',
    'Apps Script version created',
    'Test deployment created as **Editor add-on**',
    'QA spreadsheet install succeeds',
    '`MaxCompute` menu appears',
    'Settings sidebar opens',
    'Query sidebar opens'
  ]);

  requireRows(failures, evidence, [
    '`runReleaseSmokeTests()`',
    '`test_connectionStatus()`',
    '`test_readOnlySqlGuard()`',
    '`test_endpointValidation()`',
    '`test_listSchemas()`',
    '`test_listTables()`',
    '`test_getTableSchema()`',
    '`test_listPartitions()`',
    '`test_executeSimpleQuery()`',
    '`test_odpsSignature()`'
  ], { allowNa: true, cellIndex: 0 });

  requireRows(failures, evidence, [
    'Save settings with QA AK/SK/project/endpoint',
    'Test connection from settings sidebar',
    'Run `SELECT 1 AS id;` from query sidebar',
    'Run a query returning no tabular result if available',
    'Run `DROP TABLE t;`',
    'Run `SHOW CREATE TABLE <qa_table>;` if available',
    'Run a long query, then Cancel',
    'Attach to completed Instance ID',
    'Browse catalog schema/table/columns',
    'Load partitions for partitioned table',
    'Switch language'
  ], { allowNa: true });

  requireRows(failures, evidence, [
    '`EXT_PLATFORM_ID`',
    '`EXT_NODE_ONDUTY`',
    '`EXT_NODE_NAME`',
    '`EXT_NODE_ID`',
    '`EXT_TASK_ID`',
    'Submitted SQL',
    'Logview URL'
  ]);

  requireRows(failures, evidence, [
    'Marketplace SDK app configuration points to the Editor add-on Apps Script version',
    'App visibility is set for internal/domain-limited test before public release',
    'Required listing fields are complete',
    'Listing assets uploaded',
    'Data access / security disclosure completed'
  ]);

  for (const label of ['Privacy policy URL works', 'Terms of service URL works', 'Support URL works']) {
    const line = findLineContaining(evidence, label);
    if (!/https:\/\//.test(line)) {
      failures.push(`${label} must include a public HTTPS URL.`);
    } else {
      requireRowResult(failures, evidence, label);
    }
  }

  const oauthVerificationLine = findLineContaining(evidence, 'OAuth verification submitted/approved if required');
  if (!/\b(approved|not required|n\/a)\b/i.test(oauthVerificationLine)) {
    failures.push('OAuth verification evidence must say approved, not required, or N/A.');
  }

  const marketplaceReviewLine = findLineContaining(evidence, 'Marketplace review submitted/approved for public release');
  if (!/\bapproved\b/i.test(marketplaceReviewLine)) {
    failures.push('Marketplace review evidence must explicitly say approved for public release.');
  }

  if (!hasCheckedItem(evidence, 'Public Marketplace release')) {
    failures.push('Final decision must check "[x] Public Marketplace release".');
  }
  if (hasCheckedItem(evidence, 'Hold release')) {
    failures.push('Final decision cannot also check "Hold release".');
  }
  if (hasCheckedItem(evidence, 'Internal/domain-limited release only')) {
    failures.push('Final decision cannot also check "Internal/domain-limited release only".');
  }

  requireRows(failures, evidence, [
    'Are all repository gates green?',
    'Are all Apps Script QA gates green or explicitly N/A?',
    'Are all real MaxCompute smoke tests green or explicitly N/A?',
    'Are OAuth and Marketplace gates complete for the target visibility?',
    'Public release approved by owner?'
  ]);

  return {
    ok: failures.length === 0,
    failures,
    evidencePath,
    manifestPath
  };
}

if (require.main === module) {
  const result = verifyPublicRelease();
  if (!result.ok) {
    console.error('Public release verification failed:');
    for (const failure of result.failures) {
      console.error(`- ${failure}`);
    }
    process.exitCode = 1;
  } else {
    console.log('Public release verification passed.');
  }
}

module.exports = {
  verifyPublicRelease
};
