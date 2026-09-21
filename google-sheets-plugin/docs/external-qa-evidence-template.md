# External QA Evidence

## Release Candidate

| Field | Value |
|-------|-------|
| Candidate version | TODO |
| Apps Script project ID | TODO |
| Standard Google Cloud project ID | TODO |
| Apps Script version number | TODO |
| Editor add-on test deployment ID | TODO |
| QA spreadsheet URL / ID | TODO |
| QA MaxCompute project | TODO |
| Tester Google account(s) | TODO |
| Test date | TODO |

## Local Repository Gates

| Gate | Evidence |
|------|----------|
| npm run release:local | TODO |
| src/appsscript.json / src/.clasp.example.json / dist/apps-script/appsscript.json / dist/apps-script/.clasp.json / package.json JSON parse | TODO |
| Production package excludes QA functions (dist/apps-script/Test.js absent) | TODO |
| Git commit / tag under test | TODO |

For the Editor add-on test deployment ID, do not use a Google Workspace add-on deployment ID.

## Apps Script Deployment Gates

| Gate | Expected Evidence | Result |
|------|-------------------|--------|
| Apps Script is attached to a standard Google Cloud project | Cloud project number is set in Apps Script project settings | TODO |
| OAuth consent app exists for the same Cloud project | Consent screen app name, support email, privacy URL, terms URL configured | TODO |
| OAuth scopes match src/appsscript.json | https://www.googleapis.com/auth/spreadsheets; https://www.googleapis.com/auth/script.container.ui; https://www.googleapis.com/auth/script.external_request; https://www.googleapis.com/auth/script.storage; https://www.googleapis.com/auth/script.scriptapp; https://www.googleapis.com/auth/userinfo.email | TODO |
| Apps Script version created | Immutable version number recorded above | TODO |
| Test deployment created as **Editor add-on** | Test deployment ID recorded above | TODO |
| QA spreadsheet install succeeds | Add-on available from Extensions / test deployment flow | TODO |
| MaxCompute menu appears | Screenshot archived | TODO |
| Settings sidebar opens | Screenshot archived | TODO |
| Query sidebar opens | Screenshot archived | TODO |

## Apps Script QA Functions

| Function | Result | Evidence |
|----------|--------|----------|
| runReleaseSmokeTests() | TODO | TODO |
| test_connectionStatus() | TODO | TODO |
| test_readOnlySqlGuard() | TODO | TODO |
| test_endpointValidation() | TODO | TODO |
| test_listSchemas() | TODO | TODO |
| test_listTables() | TODO | TODO |
| test_getTableSchema() | TODO | TODO |
| test_listPartitions() | TODO | TODO |
| test_executeSimpleQuery() | TODO | TODO |
| test_odpsSignature() | TODO | TODO |

## Google Sheets UI Smoke Tests

| Scenario | Expected Result | Result |
|----------|-----------------|--------|
| Save settings with QA AK/SK/project/endpoint | Success message | TODO |
| Test connection from settings sidebar | Success message | TODO |
| Run SELECT 1 AS id; from query sidebar | Result sheet created | TODO |
| Run a query returning no tabular result if available | Status row written | TODO |
| Run DROP TABLE t; | Rejected before submission | TODO |
| Run SHOW CREATE TABLE <qa_table>; if available | Rejected by the SELECT/WITH-only guard | TODO |
| Run a long query, then Cancel | Cancel request sent | TODO |
| Attach to completed Instance ID | Result fetched | TODO |
| Browse catalog schema/table/columns | Catalog expands | TODO |
| Load partitions for partitioned table | Partitions render | TODO |
| Switch language | Labels update | TODO |

Additional current-feature QA (each needs real Apps Script and destination evidence):

| Scenario | Expected Result | Result |
|----------|-----------------|--------|
| Switch sheets while submit callback is pending | Result remains bound to submitting sheet | TODO |
| Disable SQL history and reopen sidebar | Old entries cleared; future SQL not saved | TODO |
| Install, execute and remove schedule trigger | Correct spreadsheet reopened; disabled trigger stops | TODO |
| Export CSV to authorized OSS bucket | Selected data and encoding verified; no arbitrary host access | TODO |
| Cancel returns a failed kill response | Failure visible; polling continues | TODO |

## MaxCompute Audit Evidence

| Field | Expected | Evidence |
|-------|----------|----------|
| EXT_PLATFORM_ID | Gsheet | TODO |
| EXT_NODE_ONDUTY | Submitting Google account email | TODO |
| EXT_NODE_NAME | Google Spreadsheet name if available | TODO |
| EXT_NODE_ID | Google Spreadsheet ID | TODO |
| EXT_TASK_ID | Target Sheet name | TODO |
| Submitted SQL | User SQL is read-only and audit EXT fields are in task settings | TODO |
| Logview URL | Opens the matching Instance ID | TODO |

## Marketplace / OAuth Gates

| Gate | Expected Evidence | Result |
|------|-------------------|--------|
| Marketplace SDK app configuration points to the Editor add-on Apps Script version | Exact candidate version number | TODO |
| App visibility is set for internal/domain-limited test before public release | Test users can install | TODO |
| Required listing fields are complete | App name, short description, detailed description, category, support email, website | TODO |
| Listing assets uploaded | Icons and screenshots match the current UI | TODO |
| Privacy policy URL works | Public URL opens | TODO |
| Terms of service URL works | Public URL opens | TODO |
| Support URL works | Public URL opens | TODO |
| Data access / security disclosure completed | Matches docs/marketplace-submission-draft.md | TODO |
| OAuth verification submitted/approved if required | Verification status recorded | TODO |
| Marketplace review submitted/approved for public release | Review status recorded | TODO |

## Release Decision

| Question | Answer |
|----------|--------|
| Are all repository gates green? | TODO |
| Are all Apps Script QA gates green or explicitly N/A? | TODO |
| Are all real MaxCompute smoke tests green or explicitly N/A? | TODO |
| Are OAuth and Marketplace gates complete for the target visibility? | TODO |
| Public release approved by owner? | TODO |

Final decision:

- [x] Hold release
- [ ] Internal/domain-limited release only
- [ ] Public Marketplace release

Notes:

```text
TODO: Hold release until independent external evidence and owner approval exist.
```
