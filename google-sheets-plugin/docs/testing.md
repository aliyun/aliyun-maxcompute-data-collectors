# Testing Strategy

This project uses local Node tests for non-UI quality gates. The tests run
Apps Script source files inside a small VM harness and mock the Apps Script
runtime (`Utilities`, `XmlService`, `SpreadsheetApp`, `PropertiesService`,
`UrlFetchApp`, `LockService`, `HtmlService`, and `Session`).

## Commands

```bash
npm test
npm run release:local
npm run release:verify-package
npm run release:verify-public
```

Use `npm test` while developing. Use `npm run release:local` for a release
candidate; it runs the local test suite and JSON parses `src/appsscript.json`,
`src/.clasp.example.json`, the generated production package JSON files under
`dist/apps-script/`, and `package.json` so the output can be pasted into
`docs/external-qa-evidence-template.md`.

`npm run release:verify-package` checks an already generated `dist/apps-script/`
package. It verifies the exact production file list, JSON files, clasp
extension mapping, the exact allowed OAuth scope set, required MaxCompute
endpoint whitelist entries, excluded QA files, and the allowed public Apps
Script callable surface.

Use `npm run release:verify-public` only after
`docs/external-qa-evidence-template.md` has been filled with real Apps Script,
Google Sheets, MaxCompute, OAuth, and Marketplace evidence. It is expected to
fail while the evidence file still contains `TODO` values or the final decision
is not checked as public Marketplace release.

`npm run build:release` creates `dist/apps-script/` for production deploys. The
generated package excludes `src/Test.js` so Apps Script editor QA functions are
available in the repository but are not pushed as production browser-callable
server functions.

## Current Coverage

The local suite covers:

- Editor add-on lifecycle:
  - `onOpen()` builds the expected Google Sheets add-on menu
  - `onInstall()` delegates to `onOpen()`
- Apps Script manifest:
  - V8 runtime and Stackdriver exception logging
  - required Editor add-on OAuth scopes
  - current-spreadsheet scope instead of broad Spreadsheet/Drive scopes
  - MaxCompute endpoint whitelist shape
- Clasp deployment config:
  - `src/.clasp.example.json` maps `.js`/`.gs` to scripts, `.html` to HTML, and
    `.json` to JSON using clasp's separate extension settings
  - the production build under `dist/apps-script/` keeps required runtime files
    and excludes `Test.js`
  - `npm run release:verify-package` re-checks the generated production package
    before deployment
- Apps Script source syntax:
  - all `.js` files under `src/` parse as Apps Script V8 JavaScript
- ODPS V1 signing helpers:
  - canonical resource query ordering
  - empty flag query parameters
  - path/query URL encoding without double-encoding
  - dynamic project/table/instance path segments are encoded without path splitting
  - canonical string ODPS header ordering
  - case-insensitive header lookup
- SQL job XML:
  - SQL XML escaping
  - task-name XML escaping
- Config safety:
  - settings UI config never returns Secret/Token plaintext
  - user properties take precedence over legacy script properties
  - empty user properties override legacy script properties instead of falling back
  - legacy script properties are still read as a fallback
  - endpoint must be a MaxCompute HTTPS API endpoint
  - Settings UI rejects invalid custom endpoints before save/test calls
  - runtime query, progress/result/cancel, and catalog entrypoints reject
    invalid saved endpoints before HTTP calls
  - settings fields are length-capped on both client and server before
    persistence or connection-test requests
  - saving settings preserves existing Secret/Token when inputs are blank
  - Security Token can be explicitly cleared
  - clearing Security Token overrides any legacy script-level token fallback
  - connection test restores the original config after success or failure
  - connection test does not persist user config when only legacy script config exists
- MaxCompute audit identifiers:
  - `EXT_*` values are submitted as SQLTask settings, not prepended SQL
  - empty settings behavior without audit context
  - Google Spreadsheet name in `EXT_NODE_NAME` when available
  - submitting Google account email in `EXT_NODE_ONDUTY`
  - target Sheet name in `EXT_TASK_ID`
  - single quotes remain valid inside JSON settings
  - newline/tab normalization
  - capped Google Spreadsheet ID in `EXT_NODE_ID`
  - length caps for EXT fields
  - sanitized task name
- Read-only SQL guard:
  - allows `SELECT`, `WITH`, metadata queries, and leading `SET`
  - allows metadata text such as `SHOW CREATE TABLE` and `SHOW GRANTS`
  - rejects user SQL longer than 65536 characters before any HTTP request
  - rejects DDL/DML/permission/load statements before any HTTP request
  - rejects other non-read-only or side-effect commands such as `ANALYZE`, `CALL`, `USE`, `BEGIN`, `COMMIT`, and `ROLLBACK`
  - rejects DML and side-effect variants hidden under allowed keywords, including
    `EXPLAIN INSERT`, external/materialized object DDL, resource add/remove,
    package install/uninstall, and MSCK repair
  - rejects multiple non-`SET` statements
  - ignores forbidden keywords in comments, strings, and quoted identifiers
- Sidebar read-only precheck:
  - browser script contains a client-side precheck for early feedback
  - SQL input is capped at 65536 characters and checked before submission
  - backend `assertReadOnlySql_()` remains the enforced boundary before HTTP submission
- Sidebar catalog SQL generation:
  - generated identifiers are quoted before insertion
  - single-level partition tables use `MAX_PT`
  - multi-level partition tables use nested `MAX()` subqueries because `MAX_PT`
    returns the maximum value of the first partition level only
- Sidebar Instance ID precheck:
  - attach input rejects empty, overlong, malformed, and path-like IDs before calling backend status APIs
  - backend `normalizeInstanceId_()` remains the enforced boundary before status/result/cancel HTTP calls
- Sidebar local history controls:
  - recent SQL history is stored in browser `localStorage`
  - individual entries can be deleted
  - local SQL history saving can be disabled, which clears existing history and
    prevents successful future SQL from being persisted
  - the clear-history control confirms with the user and removes `mc_sql_history`
- XML/result parsers:
  - instance status
  - task status
  - Base64 result payloads
  - failed result payloads
  - ODPS error XML
- URL helpers:
  - Logview region extraction
  - Logview query parameter encoding
  - ODPS fetch query objects are not mutated while adding `curr_project`
  - safe JSON serialization for data embedded in `<script>`
- Logging/privacy:
  - SQL logs record length and first keyword instead of raw SQL
  - async execution logs Instance ID length instead of raw Instance ID
  - legacy synchronous timeout, failed, and abnormal-status errors include
    Instance ID length only, not the raw Instance ID
  - Sidebar failure/cancel/abnormal-status messages summarize Instance ID length
    instead of echoing raw Instance IDs
  - result parse failures surface safe `messageLen` errors instead of raw parser
    exception text
  - cancel responses return Instance ID length instead of the raw Instance ID
  - entrypoint logs summarize spreadsheet, project, and sheet identifiers by length
  - QA helper logs and smoke summaries summarize catalog identifiers by count/length
  - non-SQL API error bodies are summarized by code/message length before surfacing
  - connection-test failures preserve known-safe Schema catalog summaries while
    hiding raw business identifiers
  - catalog XML/JSON parser failures surface safe `messageLen` errors to the UI
    instead of silently rendering empty catalog results
  - catalog responses with an unexpected XML root are also rejected with a safe
    `messageLen` error instead of being treated as an empty catalog
  - catalog table JSON schema responses with an unexpected shape are rejected
    with a safe `messageLen` error instead of being treated as zero columns
  - Sidebar preserves safe catalog error summaries so users can distinguish
    schema, table, table-detail, and partition failures without exposing raw names
  - clicking a catalog table surfaces table-detail load failures instead of
    silently generating a fallback `SELECT *`
  - catalog error rendering falls back to the result panel if the target tree
    node no longer exists after refresh or another async state change
  - catalog state maps use null prototypes so schema/table names such as
    `__proto__` and `constructor` are handled as ordinary data keys
  - ODPS request logs avoid full resource URLs and business identifiers
- Sidebar HTML rendering safety:
  - loading text uses `textContent` except for explicitly trusted Logview link HTML
  - success summaries escape server-provided row/column counts, sheet names,
    Instance IDs, and Logview URLs before composing result HTML
  - known-safe catalog error summaries are shown directly while raw catalog
    failures are still collapsed to `Server error: messageLen=N`
- Mock contract tests:
  - SQL submit posts to `/instances`
  - SQL submit body includes audit EXT fields
  - 201 Location response returns instance ID
  - 201 Location response rejects invalid Location-derived instance IDs
  - synchronous polling timeout input is bounded to 1-300 seconds
  - synchronous timeout/failed/abnormal status errors avoid raw Instance IDs
  - synchronous HTTP 200 result writes directly to Sheet
  - non-2xx submit parses ODPS error
  - running progress returns non-terminal state
  - failed progress includes error message
  - cancellation sends a terminate-Instance PUT request
  - cancellation and terminal failure UI states display Instance ID length only
  - result fetching includes `curr_project`
  - table listing uses `curr_schema`, `prefix`, and `maxitems`
  - table schema requests include `asynccache` and `curr_schema`
  - partition listing errors are surfaced instead of silently returning empty data
  - connection-test Schema catalog failures remain actionable without exposing
    raw schema/project identifiers
  - catalog parser failures are surfaced as safe message-length errors instead
    of empty schema/table/detail/partition results
  - catalog responses with the wrong XML root are rejected instead of being
    interpreted as empty schema/table/detail/partition results
  - catalog JSON schema responses with the wrong shape are rejected instead of
    being interpreted as empty table schemas
  - table-click SQL generation does not hide table-detail load failures behind
    a fallback `SELECT *`
  - catalog error rendering tolerates missing/stale DOM containers
  - result writing creates header and rows in the target sheet
  - empty tabular results write a clear status row instead of throwing
  - result parse failures do not create a target Sheet and do not expose raw
    result/parser context
  - async result writing respects the requested row limit
  - result preparation caps written rows at 10,000
  - result writes use a document lock and fail clearly when the lock is unavailable
- HTML syntax gates:
  - `Sidebar.html` browser script parses after Apps Script template normalization
  - `Settings.html` browser script parses
  - every `google.script.run` call in Sidebar/Settings targets the production
    Apps Script callable allowlist
- Apps Script QA helpers:
  - `src/Test.js` includes local safety checks for read-only SQL and endpoint validation
  - `src/Test.js` includes real-service smoke tests for connection, catalog, SQL, and signing
  - `runReleaseSmokeTests()` returns a structured summary and fails fast on required smoke gates
- Public release evidence verifier:
  - `npm run release:verify-public` checks the filled external QA evidence file
  - it requires a clean release commit/tag, local pass counts, `json ok`,
    `excluded: Test.js`, Apps Script version/deployment evidence, Apps Script
    QA rows, Google Sheets UI smoke rows, MaxCompute audit rows, manifest OAuth
    scopes, public support/privacy/terms URLs, OAuth/Marketplace approval
    evidence, and a final public-release decision
- Production package verifier:
  - `npm run release:verify-package` rejects QA-only files, broad or unknown
    OAuth scopes, missing or duplicate required MaxCompute whitelist entries,
    unexpected public callables, and forbidden Apps Script APIs that would widen
    data access beyond the manifest/release design

## Out Of Scope

These local tests intentionally do not open Google Sheets UI and do not call
real MaxCompute services. Release smoke tests can be added separately for:

- QA MaxCompute connection/catalog check
- simple query written to a QA Google Sheet
- `tasks_history` verification for `EXT_PLATFORM_ID`, `EXT_NODE_ID`,
  `EXT_NODE_NAME`, `EXT_TASK_ID`, and `EXT_NODE_ONDUTY`
