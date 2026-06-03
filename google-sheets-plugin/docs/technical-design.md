# MaxCompute Google Sheets Plugin - Technical Design

## 1. Project Overview

### 1.1 Product Positioning

The MaxCompute Google Sheets Plugin is a Google Sheets Add-on that allows users to query data from Alibaba Cloud MaxCompute (formerly ODPS) data warehouse directly within Google Sheets, without writing code or exporting data files.

### 1.2 Target Users

- **Data Analysts**: Query big data warehouses directly in familiar spreadsheet tools
- **Business Operations Staff**: Quickly retrieve business data via SQL
- **Developers**: Rapidly validate data and debug SQL

### 1.3 Core Value Proposition

| Value Point | Description |
|-------------|-------------|
| Zero Deployment Cost | Based on Google Apps Script, no standalone server required |
| Native Integration | Results written directly to Sheet, supporting sorting, filtering, visualization |
| Secure Authentication | Uses Alibaba Cloud AccessKey + HMAC-SHA1 signature mechanism |
| Cross-Region Support | Covers China, Asia Pacific, Europe, Americas, and other global regions |

---

## 2. Technical Architecture

### 2.1 Overall Architecture Diagram

```
┌──────────────────────────────────────────────────────┐
│                   Google Sheets UI                    │
│  ┌─────────────────┐    ┌──────────────────────────┐ │
│  │   Sidebar.html  │    │     Settings.html        │ │
│  │  (Query Panel)  │    │     (Config Panel)       │ │
│  │                 │    │                          │ │
│  │  • SQL Input    │    │  • AK/SK Config          │ │
│  │  • Execute Btn  │    │  • Project/Endpoint      │ │
│  │  • Data Catalog │    │  • Connection Test       │ │
│  │  • Results      │    │                          │ │
│  └────────┬────────┘    └──────────┬───────────────┘ │
└───────────┼────────────────────────┼─────────────────┘
            │ google.script.run      │
            ▼                        ▼
┌──────────────────────────────────────────────────────┐
│              Google Apps Script Backend               │
│                                                      │
│  Code.js          ← Entry: Menus, Sidebar, Routing   │
│  SqlExecutor.js   ← SQL Submission, Polling, Parsing │
│  TableBrowser.js  ← Schema/Table/Partition Browsing  │
│  OdpsSigner.js    ← ODPS V1 Signature Calculation    │
│  Config.js        ← Config Management (PropertiesService) │
│                                                      │
│  Dependent Services:                                 │
│  • SpreadsheetApp  → Manipulate Google Sheets        │
│  • UrlFetchApp     → HTTP Requests                   │
│  • XmlService      → XML Parsing                     │
│  • PropertiesService → Persistent Storage            │
│  • HtmlService     → Render Sidebar                  │
│  • Utilities       → HMAC/Base64/MD5                 │
└────────────────────────┬─────────────────────────────┘
                         │ HTTPS (HMAC-SHA1 Signature)
                         ▼
┌──────────────────────────────────────────────────────┐
│           Alibaba Cloud MaxCompute API                │
│                                                      │
│  Instance Job API:                                   │
│  • POST /projects/{project}/instances    Submit SQL  │
│  • GET  /projects/{project}/instances/{id} Status    │
│  • GET  ...?taskstatus                   Task Status │
│  • GET  ...?result                       Get Results │
│                                                      │
│  Data Catalog API:                                   │
│  • GET /projects/{project}/schemas       Schema List │
│  • GET /projects/{project}/tables        Table List  │
│  • GET /projects/{project}/tables/{name} Table Schema│
│  • GET ...?partitions&name               Partition List│
└──────────────────────────────────────────────────────┘
```

### 2.2 Module Division

| Module | File | Responsibility |
|--------|------|----------------|
| **Entry Layer** | `Code.js` | Menu creation, sidebar rendering, function routing, language switching |
| **SQL Executor** | `SqlExecutor.js` | SQL job submission, status polling, result parsing, XML processing |
| **Data Catalog** | `TableBrowser.js` | Schema/table/partition metadata queries, XML/JSON schema parsing |
| **Signature & Auth** | `OdpsSigner.js` | ODPS V1 signature algorithm, HTTP request encapsulation |
| **Config Management** | `Config.js` | Config read/save/masking, connection testing |
| **UI Templates** | `Sidebar.html` / `Settings.html` | Frontend interface, async call logic |

### 2.3 Technology Selection

| Technology | Choice | Reason |
|------------|--------|--------|
| Runtime | Google Apps Script (V8) | Deep integration with Google Sheets, zero maintenance |
| Language | JavaScript (ES5+) | Natively supported by Apps Script |
| HTTP Client | UrlFetchApp | Built-in to Apps Script, supports whitelist control |
| Data Storage | PropertiesService | Apps Script key-value store, suitable for small configs |
| Frontend Framework | Vanilla HTML/CSS/JS | Lightweight, no extra dependencies |
| XML Parsing | XmlService | Built-in to Apps Script |

---

## 3. Core Process Design

### 3.1 SQL Query Execution Flow

```
User inputs SQL
    │
    ▼
[1] Parameter validation & config check
    │   • Client-side read-only precheck for faster feedback
    │   • Server-side read-only guard before UrlFetchApp.fetch()
    │
    ▼
[2] Submit SQL job (POST /instances)
    │
    ├── HTTP 200 → Synchronous execution complete
    │      │
    │      ▼
    │   Parse results directly
    │
    └── HTTP 201 → Async job created successfully
           │
           ▼
[3] Extract Instance ID from Location Header
           │
           ▼
[4] Frontend polls Instance status (GET /instances/{id})
    │   Polling strategy: fixed incremental interval [1s, 2s, 4s, 8s]
    │   Each poll is a separate Apps Script call
    │
    ▼
[5] Instance status = Terminated
    │
    ▼
[6] Query Task status (GET ...?taskstatus)
    │
    ├── Success → Get results
    ├── Failed  → Throw exception (with error message)
    └── Other   → Throw exception (status anomaly)
    │
    ▼
[7] Parse result XML (GET ...?result)
    │   • Parse ResultDescriptor (JSON) to get column names
    │   • Parse CSV data body
    │   • Handle \N (NULL) replacement
    │
    ▼
[8] Write to Google Sheet
    │   • Create/clear target sheet
    │   • Write header (blue background, white font, bold)
    │   • Cap written rows at 10,000
    │   • Serialize writes with document lock
    │   • Alternating row colors (when ≤1,000 rows)
    │   • Freeze header, auto-fit columns
    │
    ▼
[9] Return execution summary to frontend
    │   • rowCount, columnCount
    │   • instanceId, logviewUrl
    │   • sheetName
```

### 3.2 Dual-Mode Execution Strategy

| Mode | Trigger Condition | Use Case |
|------|-------------------|----------|
| **Synchronous** | MaxCompute returns HTTP 200 | Small read-only queries and metadata queries |
| **Asynchronous** | MaxCompute return HTTP 201 | Large queries, long-running jobs |

In synchronous mode, results are returned directly in the request response; asynchronous mode requires polling to obtain final results.

The sidebar also supports:

- Manual cancellation through `cancelQuery(instanceId)`, which best-effort terminates the MaxCompute Instance.
- Attaching to an existing Instance ID, so a user can recover a running or completed job after the sidebar is closed.

### 3.3 Data Catalog Browsing Flow

```
Frontend requests schema list
    │
    ▼
[1] GET /projects/{project}/schemas?maxitems=1000
    │
    ▼
[2] Parse XML → Return { name, owner, creationTime }[]
    │
    ▼
User expands a schema
    │
    ▼
[3] GET /projects/{project}/tables?curr_schema={schema}&prefix={prefix}
    │
    ▼
[4] Parse XML → Return { name, type, comment }[]
    │
    ▼
User clicks table name
    │
    ▼
[5] GET /projects/{project}/tables/{table}?asynccache&curr_schema={schema}
    │
    ▼
[6] Parse table schema (supports both JSON/XML formats)
    │   • Regular column list
    │   • Partition column list
    │
    ▼
[7] Frontend generates SELECT SQL statement
    │
    ▼
User clicks "show partitions" (partitioned table)
    │
    ▼
[8] GET /projects/{project}/tables/{table}?partitions&name&curr_schema={schema}
    │
    ▼
[9] Parse partition list → Display in frontend
```

---

## 4. Key Technical Implementation

### 4.1 ODPS V1 Signature Algorithm

#### Signature Formula

```
StringToSign = HTTPMethod + "\n"
             + Content-MD5 + "\n"
             + Content-Type + "\n"
             + Date + "\n"
             + CanonicalizedODPSHeaders +
             + CanonicalizedResource

Signature = Base64(HMAC-SHA1(AccessKeySecret, StringToSign))

Authorization = "ODPS " + AccessKeyId + ":" + Signature
```

#### Key Rules

1. **Date Header**: Must use GMT time, format `EEE, dd MMM yyyy HH:mm:ss 'GMT'`
2. **CanonicalizedODPSHeaders**: All headers starting with `x-odps-*`, sorted alphabetically by key
3. **CanonicalizedResource**: URL path + query parameters (keys sorted alphabetically)
4. **Content-MD5**: Empty in current implementation (not needed for GET requests)

#### Example Request Header

```http
GET /projects/my_project/tables?curr_schema=default&maxitems=1000 HTTP/1.1
Host: service.cn-shanghai.maxcompute.aliyun.com
Date: Tue, 31 Mar 2026 12:00:00 GMT
Content-Type: application/xml
x-odps-security-token: <STS Token>
Authorization: ODPS <AccessKeyId>:Base64Signature...
```

### 4.2 XML Parsing Strategy

MaxCompute API responses primarily use XML format, parsed using `XmlService` DOM parsing:

```javascript
// Generic child element text extraction
function getChildText_(parent, childName) {
  if (!parent) return '';
  var child = parent.getChild(childName);
  return child ? child.getText() : '';
}
```

#### Schema Format Compatibility

The `Schema` element in table structure can be in two formats:

| Format | Identification | Parsing Method |
|--------|----------------|----------------|
| **JSON** | `<Schema format="Json">` | `JSON.parse()` then iterate |
| **XML** | `<Schema><Column>...</Column></Schema>` | XmlService recursive parsing |

### 4.3 Polling Strategy

| Parameter | Value | Description |
|-----------|-------|-------------|
| Initial Interval | 1s | Wait 1 second for first poll |
| Incremental Sequence | [1s, 2s, 4s, 8s] | Exponential backoff, max 8s |
| Execution model | Frontend-driven polling | Each poll is a separate `google.script.run` call, avoiding one long blocking Apps Script invocation |
| Termination Condition | Instance Status = `Terminated` | Indicates job completion (success/failure/cancelled) |

### 4.4 Result Writing Optimization

| Optimization | Implementation | Effect |
|--------------|----------------|--------|
| **Batch Writing** | 10,000 rows per batch | Avoid exceeding Apps Script execution time limit |
| **Header Styling** | Blue background + white bold font | Visual distinction |
| **Alternating Colors** | Enabled when ≤1,000 rows | Improved readability |
| **Freeze Header** | `sheet.setFrozenRows(1)` | Header stays fixed during scrolling |
| **Auto-Fit Columns** | Enabled when ≤20 columns | Avoid performance issues with too many columns |
| **Null Handling** | `\N` → empty string | Unified NULL representation |

---

## 5. Security Design

### 5.1 Credential Management

| Aspect | Measure |
|--------|---------|
| **Storage** | Stored per user with `PropertiesService.getUserProperties()`; legacy script-level properties are read only as a fallback |
| **Transmission** | Full-chain HTTPS, MaxCompute Endpoint enforces TLS |
| **Masking** | Settings page never returns Secret/Token plaintext; configured values are shown only as masked status |
| **STS Support** | Supports temporary security tokens (`x-odps-security-token`) |

### 5.2 Read-Only SQL Policy

The add-on is intentionally query-only. DDL/DML, permission-changing, load/unload, and common side-effect statements are blocked.

| Version | Where it runs | Role |
|---------|---------------|------|
| **V1 (current)** | Client side: `Sidebar.html` browser precheck + `SqlExecutor.assertReadOnlySql_()` enforced before `UrlFetchApp.fetch()` | Heuristic first-keyword + nested side-effect regex. The browser layer is UX only; the Apps Script layer is the real boundary. |
| **V2 (planned)** | MaxCompute server side | Enforced via RAM role / read-only credential / platform-level query permission isolation. Eliminates heuristic misclassification at the root. |

Allowed statements: leading `SET` (zero or more), followed by one `SELECT` or `WITH … SELECT` — the only true result-set DQL. Blocked examples: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `GRANT`, `REVOKE`, `LOAD`, `UNLOAD`, `ANALYZE`, `CALL`, `USE`, `BEGIN` / `COMMIT` / `ROLLBACK`, metadata / explanatory statements (`SHOW`, `DESC`, `DESCRIBE`, `EXPLAIN`), and side-effect variants hidden under `WITH`.

The full algorithm (statement splitting, literal masking, first-keyword extraction, nested side-effect regex list), misclassification cases, and V1→V2 migration notes live in [docs/read-only-sql-guard.md](read-only-sql-guard.md).

### 5.3 URL Whitelist

`appsscript.json` pre-configures MaxCompute public endpoint whitelists for global regions:

| Region | Endpoint |
|--------|----------|
| Mainland China | cn-hangzhou, cn-shanghai, cn-beijing, cn-shenzhen, cn-hongkong, etc. |
| Asia Pacific | ap-southeast-1/3/5, ap-northeast-1/2 |
| Europe | eu-central-1, eu-west-1 |
| Americas | us-west-1, us-east-1, na-south-1 |
| Middle East | me-east-1, me-central-1 |

### 5.4 OAuth Scopes

```json
{
  "oauthScopes": [
    "https://www.googleapis.com/auth/spreadsheets.currentonly",  // Current spreadsheet only
    "https://www.googleapis.com/auth/script.container.ui",       // Container-bound UI
    "https://www.googleapis.com/auth/script.external_request",   // External HTTP requests
    "https://www.googleapis.com/auth/script.storage",            // Property storage
    "https://www.googleapis.com/auth/userinfo.email"             // Submitter email for job audit
  ]
}
```

Principle of least privilege: `spreadsheets.currentonly` restricts access to only the currently open spreadsheet. The `userinfo.email` scope is used only to set MaxCompute job audit metadata for the submitting user.

---

## 6. Internationalization Design

### 6.1 Language Storage

User language preference is stored in `PropertiesService.getUserProperties()`:

```javascript
// Store
props.setProperty('MC_LANGUAGE', lang);  // 'zh' or 'en'

// Retrieve
var lang = props.getProperty('MC_LANGUAGE') || 'en';
```

### 6.2 Translation Strategy

| Location | Translation Method |
|----------|--------------------|
| **Server Messages** | Dynamically concatenate Chinese/English based on `getUserLanguage()` |
| **Frontend Interface** | Embed two sets of copy in Sidebar.html, toggle display via CSS class |
| **Error Prompts** | Unified format: Chinese prefix + original error message |

---

## 7. Development & Release

### 7.1 Local Development Workflow

```bash
# 1. Clone
git clone <repository-url>
cd google-sheet-plugin

# 2. Local quality gates + production build
npm run release:local
# → produces dist/apps-script/

# 3. Push to an Apps Script project with clasp
npm install -g @google/clasp
cd dist/apps-script
clasp login
clasp create --type sheets   # or link to an existing Apps Script project
clasp push
clasp open                   # open the editor for QA / debugging
```

Always deploy from `dist/apps-script/`, never directly from `src/`. The
production package excludes `src/Test.js` so Apps Script editor QA helpers do
not become public `google.script.run` callables in production.

When iterating in Apps Script editor for QA, use the `src/` project (so that
`Test.js` is available); switch to `dist/apps-script/` for release candidates.

### 7.2 Build & Verification Commands

| Command | Purpose |
|---------|---------|
| `npm test` | Run the local Node test suite (Apps Script logic, manifest, HTML syntax). |
| `npm run release:local` | Full local release gate: `npm test` + production build + `release:verify-package` + JSON parse of all manifest/clasp/package.json. Must pass for every release candidate. |
| `npm run release:verify-package` | Independently verify an existing `dist/apps-script/`: file list, JSON, clasp extension mapping, allowed OAuth scope set, required MaxCompute endpoint whitelist, excluded QA files, public callable surface, forbidden Apps Script APIs. |
| `npm run release:verify-public` | Public-release evidence verifier. Reads `docs/external-qa-evidence-template.md` and rejects TODO values, missing legal/support URLs, missing Apps Script QA, UI smoke, MaxCompute audit, OAuth/Marketplace approval evidence, or a non-public final decision. Expected to fail until external evidence is real. |

For local test coverage details, see [testing.md](testing.md).

### 7.3 Release Process

1. **Local gate**: `npm run release:local` is green.
2. **Apps Script QA**: deploy `dist/apps-script/` as an Editor add-on test
   deployment in a standard Cloud project; run `runReleaseSmokeTests()` and the
   QA helpers in `src/Test.js` against a QA MaxCompute project.
3. **Marketplace listing**: configure OAuth consent, finalize privacy / terms /
   support pages, fill the Marketplace SDK with the Apps Script version number
   (Editor add-on), upload listing assets.
4. **External evidence**: fill
   [`docs/external-qa-evidence-template.md`](external-qa-evidence-template.md)
   with real Apps Script version, deployment, QA, MaxCompute audit, and
   Marketplace approval evidence; run `npm run release:verify-public`.
5. **Public release**: submit Marketplace review; publish after approval.

### 7.4 Release Gate Documents

| Document | Use |
|----------|-----|
| [`release-checklist.md`](release-checklist.md) | English, detailed — repository gates and external Marketplace gates |
| [`发布前检查清单.md`](发布前检查清单.md) | Chinese, concise — checkbox-style pre-release checklist |
| [`release-readiness-audit.md`](release-readiness-audit.md) | Repository readiness audit (criteria → evidence → status) |
| [`completion-audit.md`](completion-audit.md) | Completion audit; tracks repo-ready vs external-pending criteria |
| [`marketplace-submission-draft.md`](marketplace-submission-draft.md) | Draft for OAuth consent screen and Marketplace SDK listing |
| [`external-qa-evidence-template.md`](external-qa-evidence-template.md) | Evidence record filled per release candidate |
| [`privacy-policy-template.md`](privacy-policy-template.md) · [`terms-of-service-template.md`](terms-of-service-template.md) · [`support-page-template.md`](support-page-template.md) | Public legal / support page templates |

### 7.5 Version Management

- Use Git for source code management.
- Use the "Version" feature inside the Apps Script project to record an
  immutable version per release candidate; the Marketplace SDK listing must
  point to that exact version number for an Editor add-on.
- Tag in Git before each release; record the tag in
  `docs/external-qa-evidence-template.md`.

---

## 8. Performance & Limitations

### 8.1 Google Apps Script Limitations

| Limitation | Threshold | Mitigation |
|------------|-----------|------------|
| Single execution time | 6 minutes | Frontend-driven polling keeps each Apps Script call short |
| URL Fetch calls | 20,000/day | Reasonable caching, reduce duplicate requests |
| User property storage size | 500 KB | Store only essential configurations |
| Spreadsheet cell count | 10 million | Single sheet writes up to 10,000 rows maximum |

### 8.2 MaxCompute API Limitations

| Limitation | Description |
|------------|-------------|
| **Maximum rows per query result** | **MaxCompute server returns at most 10,000 rows**; excess rows will be truncated |
| Concurrent instances | Default 200 per user |
| Polling frequency | Use incremental intervals (1s→8s) to avoid frequent requests |

> ⚠️ **Important Note**: Due to hard limits on the MaxCompute server side, a single SQL query result can return at most 10,000 rows. If the query result exceeds this number, the plugin will only fetch the first 10,000 rows. It is recommended to use the `LIMIT` clause in SQL to control the number of returned rows.

### 8.3 Frontend Performance

| Optimization | Implementation |
|--------------|----------------|
| Data Catalog Lazy Loading | Load Schema/Table/Partition hierarchically |
| Local History Cache | Use browser localStorage to store recent SQL |
| On-Demand Partition Loading | Only load when user clicks "show partitions" |

---

## 9. Testing Strategy

### 9.1 Test Case Checklist

| Test Category | Test Function | Verification |
|---------------|---------------|--------------|
| Connection test | `testConnection()` | Verify AK/SK/Project/Endpoint correctness |
| SQL execution | `testExecuteSql()` | Simple query returns results normally |
| Schema list | `test_listSchemas()` | Schema list parsing |
| Table list | `test_listTables()` | Table list under specified schema |
| Table schema | `test_getTableSchema()` | Field info, partition column parsing |
| Partition list | `test_listPartitions()` | Partition data parsing |

### 9.2 Test Execution

Select and run test functions in the Apps Script editor, view log output:
「View → Logs」

### 9.3 Manual Test Scenarios

1. **End-to-End Test**: Fill config → Open query panel → Execute `SELECT 1;` → Check results
2. **Data Catalog Test**: Expand schema → View table list → Click table name to generate SQL
3. **Language Switch Test**: Switch between Chinese/English → Verify interface copy
4. **Error Scenarios**: Intentionally enter wrong AK → Verify error prompts

---

## 10. Future Enhancements

### 10.1 Feature Enhancements

| Feature | Priority | Description |
|---------|----------|-------------|
| Cloud-synced query history | P1 | Currently localStorage only, could use PropertiesService |
| Multi-project switching | P1 | Support dropdown switching between different projects in sidebar |
| Query templates | P2 | Pre-built common query templates |
| Result export | P2 | Support export to CSV/Excel |
| Chart visualization | P3 | Leverage Google Sheets built-in chart features |

### 10.2 Technical Optimizations

| Optimization | Description |
|--------------|-------------|
| Async task queue | For ultra-long queries, use triggers for background execution |
| Result caching | Cache and reuse results from identical SQL within short timeframes |
| Incremental updates | Update only changed data rows, avoid full rewrites |
| Web App mode | Consider converting to standalone Web App, decouple from Sheets |

---

## 11. Appendix

### 11.1 File Structure

```
google-sheet-plugin/
├── AGENTS.md                  # Developer Guide
├── README.md                  # Project Documentation
├── package.json               # Local test scripts
├── docs/
│   ├── technical-design.md    # This document
│   ├── 技术方案.md             # Chinese technical design
│   ├── testing.md             # Local test strategy
│   ├── release-checklist.md   # Release gates
│   ├── marketplace-submission-draft.md
│   ├── privacy-policy-template.md
│   ├── terms-of-service-template.md
│   ├── support-page-template.md
│   └── release-readiness-audit.md
└── src/
    ├── appsscript.json        # Apps Script Manifest
    ├── .clasp.example.json    # Safe Clasp CLI Config Template
    ├── Code.js                # Main Entry: Menus, API Routing
    ├── SqlExecutor.js         # SQL Executor
    ├── TableBrowser.js        # Data Catalog
    ├── OdpsSigner.js          # ODPS V1 Signature
    ├── Config.js              # Config Management
    ├── Sidebar.html           # Query Sidebar
    ├── Settings.html          # Settings Sidebar
    └── Test.js                # Test Cases
└── scripts/
    └── build-release.js       # Builds dist/apps-script without QA Test.js
└── dist/
    └── apps-script/           # Generated production Apps Script package
└── tests/
    ├── local.test.js          # Apps Script logic tests
    ├── html-syntax.test.js    # Sidebar/settings browser script syntax tests
    ├── manifest.test.js       # Apps Script manifest checks
    └── helpers/gasHarness.js  # Local Apps Script runtime harness
```

### 11.2 Key API Reference Table

| Function | HTTP Method | Path | Query Parameters |
|----------|-------------|------|------------------|
| Submit SQL | POST | `/projects/{project}/instances` | `curr_project={project}` |
| Query Instance Status | GET | `/projects/{project}/instances/{id}` | `curr_project={project}` |
| Query Task Status | GET | `/projects/{project}/instances/{id}` | `taskstatus=&curr_project={project}` |
| Get Results | GET | `/projects/{project}/instances/{id}` | `result=&curr_project={project}` |
| Schema List | GET | `/projects/{project}/schemas` | `maxitems=1000&curr_project={project}` |
| Table List | GET | `/projects/{project}/tables` | `curr_schema={schema}&maxitems=1000&curr_project={project}` |
| Table Schema | GET | `/projects/{project}/tables/{table}` | `asynccache&curr_schema={schema}&curr_project={project}` |
| Partition List | GET | `/projects/{project}/tables/{table}` | `partitions&name&curr_schema={schema}&curr_project={project}` |

### 11.3 References

- [MaxCompute Instance Job API](https://help.aliyun.com/document_detail/27985.html)
- [MaxCompute Signature Mechanism](https://help.aliyun.com/document_detail/34951.html)
- [Google Apps Script Documentation](https://developers.google.com/apps-script)
- [Clasp CLI Tool](https://github.com/google/clasp)

---

*Document version: v1.0*
*Last updated: 2026-03-31*
