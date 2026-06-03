# MaxCompute Connector for Google Sheets

> 在 Google Sheets 中直接查询阿里云 MaxCompute（原 ODPS）数据仓库。
> Query Alibaba Cloud MaxCompute (formerly ODPS) data directly from Google Sheets.

[中文](#中文) | [English](#english) · License: Apache-2.0

---

## 中文

### 简介

**MaxCompute Connector for Google Sheets** 是一个 Google Sheets 的 Editor add-on（编辑器插件），让数据分析师、业务人员和开发者无需编写代码或导出文件，就能从 Google Sheets 内运行 MaxCompute 只读 SQL，并把结果写回当前表格。

适用人群：

- **数据分析师** — 在熟悉的电子表格里直接查 MaxCompute，结果可立即排序、过滤、画图。
- **业务运营** — 用 SQL 拉数据，无需研发协助导出。
- **开发者** — 快速验证数据、调试 SQL。

核心价值：

- **零部署成本** — 基于 Google Apps Script，无需自建服务。
- **原生集成** — 结果直接写入当前 Sheet，原生支持排序、过滤、可视化。
- **凭证安全** — AccessKey + HMAC-SHA1 签名；Secret/Token 不回传浏览器明文。
- **覆盖 MaxCompute 所有 Region** — 全球公共 endpoint 默认开通。

### 主要功能

#### 查询执行

- 侧边栏 SQL 编辑器，支持只读 SQL 和前置 `SET` 语句。
- 同步 / 异步双模式：小查询直接返回，长查询走异步 + 前端轮询，避免单次 Apps Script 调用超时。
- 结果自动写入指定 Sheet，含表头样式、首行冻结、≤1000 行时自动间隔色、≤20 列时自动适配列宽。
- 公式样式（`=`、`+`、`-`、`@`、Tab、换行开头）的字段以文本形式写入，避免被当作 Sheet 公式执行。
- Logview 链接可点击直跳 MaxCompute 控制台。

#### 数据目录浏览

- 三层结构：Project → Schema → Tables，懒加载。
- 点击表名一键生成 `SELECT` 语句；分区表自动生成 `MAX_PT`（单级）或嵌套 `MAX()` 子查询（多级）。
- 点击字段名插入到 SQL 编辑器。
- 「显示分区」按需加载分区列表。

#### 长作业与作业控制

- 异步查询提交后展示 Instance ID 和 Logview 链接，可一键 *Cancel* 终止作业。
- 侧边栏关闭/刷新后可通过 *Attach to existing job* 输入 Instance ID 继续轮询或拉取已完成结果。
- 长作业方案、Attach 使用细节、相关注意事项见：[长作业与 Attach 使用指南](docs/long-running-jobs-instance-attach.md)。

#### 作业审计

- 每条提交到 MaxCompute 的查询都会按照 [MaxCompute 通用作业标识约定](https://help.aliyun.com/zh/maxcompute/user-guide/general-operation-identification-convention) 自动写入审计字段（提交平台、Spreadsheet ID/名、目标 Sheet 名、提交者 email），便于在 MaxCompute 控制台、Logview、Information Schema 中追溯。
- 审计字段通过 MaxCompute 控制台、Logview、Information Schema 可追溯。

#### 用户体验

- 中英文双语界面，按用户偏好持久化。
- 最近查询历史保存在浏览器 `localStorage`，可禁用、单条删除、一键清空。

### 安全与隐私

| 方面 | 说明 |
|------|------|
| **凭证存储** | 按 Google 用户隔离保存；AccessKey Secret 与 Security Token 保存后不回传浏览器明文 |
| **传输** | 全程 HTTPS；Endpoint 必须为 MaxCompute 官方 API（`https://service.{region}.maxcompute.aliyun.com/api`） |
| **只读 SQL** | V1（当前）在客户端两层启发式拦截 DDL/DML/permission/load/resource/package/MSCK 等副作用语句；V2（规划）将下沉到 MaxCompute 服务端权限隔离。算法、误判、迁移说明见 [只读 SQL 限制](docs/read-only-sql-guard.md) |
| **审计字段不可伪造** | 用户 SQL 不能手动 `SET EXT_*` 字段 |
| **错误与日志脱敏** | 日志和 UI 失败信息只保留长度摘要、HTTP 码等已知安全字段，不展示原始 SQL、Project、Schema、Table、Sheet 名或 Instance ID |
| **Sheet 作用域** | 查询结果写入当前打开的电子表格 |
| **本地历史** | 查询历史只在浏览器本地存储，不上传服务端 |
| **网络白名单** | 仅允许出站到 MaxCompute 官方 endpoint，不出站到任何其它域名 |

#### OAuth 权限

| Scope | 用途 |
|-------|------|
| `spreadsheets` | 读写电子表格（定时调度需要 openById） |
| `script.container.ui` | 在 Google Sheets 中显示菜单和侧边栏 |
| `script.external_request` | 向 MaxCompute API 发送签名 HTTPS 请求 |
| `script.storage` | 保存当前用户的连接配置和语言偏好 |
| `userinfo.email` | 把提交者 Google 账号 email 写入 MaxCompute 任务审计字段 `EXT_NODE_ONDUTY` |

插件不申请 Google Drive 或全 Spreadsheet 范围的权限。

### 安装

当前以源码部署到你自己的 Apps Script 项目中使用。需要 Node.js ≥ 20、一个 Google 账号，以及 [`@google/clasp`](https://github.com/google/clasp) CLI。

```bash
# 1. 克隆仓库
git clone <repository-url>
cd google-sheet-plugin

# 2. 安装 clasp 并登录
npm install -g @google/clasp
clasp login

# 3. 本地校验 + 构建生产包到 dist/apps-script/
#    生产包不含 Test.js，确保 QA 函数不会被 google.script.run 公开调用
npm run release:local
```

下面二选一，把生产包推送到 Apps Script 项目：

**A. 新建一个绑定到新 Google Sheet 的项目**

```bash
cd dist/apps-script
clasp create --type sheets --title "MaxCompute Query" --rootDir .
clasp push
clasp open
```

`clasp create --type sheets` 会自动创建一个新的 Google Sheet 并绑定 Apps Script 项目；`clasp open` 打开 Apps Script 编辑器，从那里能直接跳到对应的 Sheet。

**B. 推送到已有 Google Sheet 的 Apps Script 项目**

1. 在目标 Google Sheet 打开「扩展程序 → Apps Script」。
2. 在 Apps Script 编辑器中：「项目设置 → 脚本 ID」复制脚本 ID。
3. 编辑 `dist/apps-script/.clasp.json`，把 `scriptId` 设为该值。
4. 在 `dist/apps-script/` 下执行 `clasp push`。

**首次打开**

回到绑定的 Google Sheet 并刷新，菜单栏会出现「MaxCompute」入口。第一次点击会弹出 OAuth 授权，授权后即可使用。

### 配置

首次使用时，从菜单「MaxCompute → 设置」打开设置侧栏：

| 字段 | 说明 | 示例 |
|------|------|------|
| AccessKey ID | 阿里云 AccessKey ID | `LTAI...` |
| AccessKey Secret | 阿里云 AccessKey Secret | `********` |
| Project | MaxCompute 项目名 | `my_project` |
| Endpoint | MaxCompute 服务端点（必须以 `/api` 结尾） | `https://service.cn-shanghai.maxcompute.aliyun.com/api` |
| Security Token | STS 临时凭证（可选） | - |

- 配置按 Google 用户隔离保存。
- AccessKey Secret 和 Security Token 保存后不再以明文返回浏览器。
- 「测试连接」会在成功/失败后恢复原配置。

### 使用方法

#### 执行 SQL 查询

1. 菜单「MaxCompute → 查询」打开查询侧栏。
2. 在 SQL 编辑框输入只读 SQL。
3. 点击 *Run Query*。
4. 结果会被写入选定的目标 Sheet。

允许的语句：若干前置 `SET`，后接一条 `SELECT` 或 `WITH … SELECT`（即真正产生结果集的 DQL）。被拒绝的语句类型：`INSERT`、`UPDATE`、`DELETE`、`MERGE`、`CREATE`、`ALTER`、`DROP`、`TRUNCATE`、`GRANT`、`REVOKE`、`LOAD`、`UNLOAD`、`ANALYZE`、`CALL`、`USE`、`BEGIN` / `COMMIT` / `ROLLBACK`、`SHOW`、`DESC`、`DESCRIBE`、`EXPLAIN` 等元数据 / 解释类语句，以及 `WITH … INSERT` 等内嵌副作用变体。完整算法、误判说明、V2 路线见 [只读 SQL 限制](docs/read-only-sql-guard.md)。

#### 浏览数据目录

1. 切换到「Data Catalog」标签。
2. 展开 Schema 查看表列表（懒加载）。
3. 点击表名 → 自动生成 `SELECT` 语句。
4. 点击字段名 → 插入字段到 SQL 编辑器。
5. 分区表点击「显示分区」按需加载分区列表。

#### 长作业 / 取消 / 恢复

异步查询的取消、关闭后恢复、最近 Instance 历史等机制详见 [长作业与 Attach 使用指南](docs/long-running-jobs-instance-attach.md)。

### 已知限制

#### 来自 MaxCompute 服务端

- **单次查询结果最多 10000 行**：MaxCompute 服务端硬限制；建议在 SQL 中 `LIMIT` 或先聚合。

#### 来自 Google Apps Script 平台

- **单次执行 6 分钟**：长查询通过前端轮询拆分；侧栏需保持打开（或重新打开后通过 Attach 恢复）。
- **UrlFetch 调用 20000 次/天**：每次提交、轮询、Catalog 操作各计 1 次。
- **用户属性 500 KB**：仅存连接配置和语言偏好，正常使用不会触及。
- **Sheet 单元格上限 1000 万**：插件单次最多写 10000 行。

#### 插件自身约束

- **只读 SQL**：不允许任何写入或副作用 SQL。
- **单条非 SET 语句**：每次提交只能跑一条只读查询；前置 `SET` 不限。
- **SQL 长度 ≤ 65536 字符**。
- **作用域限于当前电子表格**：不能跨 Sheet 写入。
- **凭证按用户隔离**：每个 Google 用户需各自配置 AccessKey；不支持共享凭证或 SSO。
- **本地查询历史不同步**：换浏览器/账号不可见。
- **暂不支持**：多 Project 切换下拉、查询模板库、CSV/Excel 导出、图表自动生成（路线图中）。

### 支持的 Region

支持 **MaxCompute 所有 Region**——`appsscript.json` 已默认为全球公共 endpoint 配置 `urlFetchWhitelist`，无需额外操作。Endpoint 输入框只接受官方域名格式 `https://service.{region}.maxcompute.aliyun.com/api`，按你 Project 所在 Region 填即可。

### 文档

| 文档 | 内容 |
|------|------|
| [docs/read-only-sql-guard.md](docs/read-only-sql-guard.md) | 只读 SQL 限制：算法、误判、V1/V2 路线 |
| [docs/long-running-jobs-instance-attach.md](docs/long-running-jobs-instance-attach.md) | 长作业与 Attach 使用指南 |
| [docs/technical-design.md](docs/technical-design.md) | 架构、签名、数据目录、开发与发布流程（面向开发者与维护者） |

### 许可证

[Apache License 2.0](LICENSE)

---

## English

### Overview

**MaxCompute Connector for Google Sheets** is a Google Sheets Editor add-on that lets data analysts, business users, and developers run read-only MaxCompute SQL from a sidebar and write the results back into the current spreadsheet — without writing code or exporting files.

Who it is for:

- **Data analysts** — query MaxCompute in the spreadsheet you already use; results are immediately ready to sort, filter, and chart.
- **Business / operations** — pull data with SQL without engineering hand-offs.
- **Developers** — quickly verify data and iterate on SQL.

Why it works:

- **Zero deployment** — runs on Google Apps Script; no separate server.
- **Native integration** — results land directly in the active sheet.
- **Credential safety** — Alibaba Cloud AccessKey + HMAC-SHA1 signing; Secret/Token never returned to the browser in plaintext.
- **Covers all MaxCompute regions** — global public endpoints whitelisted out of the box.

### Key Features

#### Query execution

- Sidebar SQL editor; supports leading `SET` followed by one read-only statement.
- Dual-mode execution: small queries return synchronously; long queries run async with frontend-driven polling, avoiding any single Apps Script call timing out.
- Results auto-written to a chosen sheet with header styling, frozen first row, alternating row color (≤1,000 rows), and auto-fit columns (≤20 columns).
- Formula-like values (`=`, `+`, `-`, `@`, leading tab/newline) are written as text, never executed as Sheets formulas.
- Clickable Logview link jumps straight to the MaxCompute console.

#### Data catalog

- Three-tier lazy browser: Project → Schema → Tables.
- Click a table to generate a `SELECT`; partitioned tables auto-use `MAX_PT` (single level) or nested `MAX()` subqueries (multi-level).
- Click a column to insert into the SQL editor.
- On-demand "show partitions" load.

#### Long-running jobs & job control

- After an async submit, the sidebar shows the Instance ID and Logview link; click *Cancel* to terminate.
- After closing or refreshing the sidebar, paste a known Instance ID into *Attach to existing job* to resume polling or fetch results.
- Detailed long-job workflow, attach behavior, and caveats: [Long-running jobs & attach guide](docs/long-running-jobs-instance-attach.md).

#### Job audit

- Every query submitted to MaxCompute is auto-tagged following the [MaxCompute generic operation identification convention](https://help.aliyun.com/zh/maxcompute/user-guide/general-operation-identification-convention) (source platform, Spreadsheet ID/name, target Sheet name, submitter email) so jobs can be traced from the MaxCompute console, Logview, and Information Schema.
- Audit fields can be traced via the MaxCompute console, Logview, and Information Schema.

#### UX

- English / Chinese sidebar, persisted per user.
- Recent SQL history is stored in browser `localStorage` only; can be disabled, deleted per item, or cleared at once.

### Security & Privacy

| Aspect | Behavior |
|--------|----------|
| **Credential storage** | Per-Google-user; AccessKey Secret and Security Token are never returned to the browser in plaintext after save |
| **Transport** | HTTPS end-to-end; the endpoint must match the official MaxCompute API form (`https://service.{region}.maxcompute.aliyun.com/api`) |
| **Read-only SQL** | V1 (current): two-layer client-side heuristic check rejects DDL/DML, permission, load/unload, resource, package, and MSCK statements before any MaxCompute call. V2 (planned): the check will move to MaxCompute server-side permission isolation. Algorithm, misclassifications, and roadmap: [Read-only SQL guard](docs/read-only-sql-guard.md) |
| **Audit fields** | User SQL cannot manually `SET EXT_*` fields |
| **Logging / failures** | Logs and UI failure messages keep only length summaries, HTTP codes, and known-safe text — never raw SQL, project, schema, table, sheet, or Instance ID |
| **Sheet scope** | Only the currently open spreadsheet is accessed (`spreadsheets.currentonly`) |
| **Local history** | Query history stays in the browser; nothing is uploaded |
| **Network whitelist** | Outbound traffic is restricted to official MaxCompute endpoints |

#### OAuth scopes

| Scope | Purpose |
|-------|---------|
| `spreadsheets` | Read/write spreadsheets (scheduled jobs require openById) |
| `script.container.ui` | Add the MaxCompute menu and HTML sidebars in Google Sheets |
| `script.external_request` | Send signed HTTPS requests to MaxCompute API endpoints |
| `script.storage` | Persist per-user connection settings and language preference |
| `userinfo.email` | Record submitter Google account email into MaxCompute audit field `EXT_NODE_ONDUTY` |

The add-on does **not** request broad Google Drive or full-spreadsheet scopes.

### Installation

The add-on is currently deployed from source into your own Apps Script project. Prerequisites: Node.js ≥ 20, a Google account, and the [`@google/clasp`](https://github.com/google/clasp) CLI.

```bash
# 1. Clone the repo
git clone <repository-url>
cd google-sheet-plugin

# 2. Install clasp and log in
npm install -g @google/clasp
clasp login

# 3. Run local checks and build the production bundle to dist/apps-script/
#    The bundle excludes Test.js so QA helpers are never exposed via google.script.run.
npm run release:local
```

Then pick one of the following to push the bundle to an Apps Script project:

**A. Create a new project bound to a new Google Sheet**

```bash
cd dist/apps-script
clasp create --type sheets --title "MaxCompute Query" --rootDir .
clasp push
clasp open
```

`clasp create --type sheets` creates a fresh Google Sheet with a bound Apps Script project; `clasp open` opens the Apps Script editor, from which you can jump to the bound sheet.

**B. Push to an existing Google Sheet's Apps Script project**

1. Open the target Google Sheet → *Extensions → Apps Script*.
2. In the Apps Script editor, *Project Settings → Script ID*, and copy the script ID.
3. Edit `dist/apps-script/.clasp.json` and set `scriptId` to the copied value.
4. Run `clasp push` from inside `dist/apps-script/`.

**First open**

Reload the bound Google Sheet; a *MaxCompute* menu appears in the menu bar. The first click triggers an OAuth consent prompt — grant the requested scopes and the menu is ready to use.

### Configuration

On first use, open *MaxCompute → Settings*:

| Field | Description | Example |
|-------|-------------|---------|
| AccessKey ID | Alibaba Cloud AccessKey ID | `LTAI...` |
| AccessKey Secret | Alibaba Cloud AccessKey Secret | `********` |
| Project | MaxCompute project name | `my_project` |
| Endpoint | MaxCompute service endpoint (must end with `/api`) | `https://service.cn-shanghai.maxcompute.aliyun.com/api` |
| Security Token | STS temporary credential (optional) | - |

- Credentials are stored per Google user.
- AccessKey Secret and Security Token are never returned to the browser in plaintext after save.
- *Test connection* restores the original config on success or failure.

### Usage

#### Run a query

1. *MaxCompute → Query Panel* opens the query sidebar.
2. Enter a read-only SQL.
3. Click *Run Query*.
4. Results are written to the chosen target sheet.

Allowed statements: leading `SET` (zero or more), followed by one `SELECT` or `WITH … SELECT` — the only true result-set DQL. Rejected statement classes include `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `GRANT`, `REVOKE`, `LOAD`, `UNLOAD`, `ANALYZE`, `CALL`, `USE`, `BEGIN`/`COMMIT`/`ROLLBACK`, metadata / explanatory statements such as `SHOW`, `DESC`, `DESCRIBE`, `EXPLAIN`, and nested side-effect variants such as `WITH … INSERT`. The full algorithm, misclassification cases, and the V2 roadmap live in [Read-only SQL guard](docs/read-only-sql-guard.md).

#### Browse the data catalog

1. Switch to the *Data Catalog* tab.
2. Expand a schema (lazy-loaded).
3. Click a table to generate a `SELECT`.
4. Click a column to insert it into the SQL editor.
5. For partitioned tables, click *show partitions* to load the partition list on demand.

#### Long jobs / cancel / attach

For async cancellation, sidebar restoration, and recent Instance history, see the dedicated [long-running jobs & attach guide](docs/long-running-jobs-instance-attach.md).

### Limitations

#### MaxCompute server-side

- **Up to 10,000 rows per query result.** This is a MaxCompute server-side hard limit; rows beyond 10,000 are not returned. Use `LIMIT` or aggregate in SQL.

#### Google Apps Script platform

- **6-minute single-execution limit.** The add-on works around this with frontend-driven polling, but the sidebar must remain open (or you can re-attach via Instance ID).
- **20,000 UrlFetch calls per day.** Each submit, each polling tick, and each catalog action counts as one call.
- **500 KB user properties.** Only connection settings and language preference are stored; not a practical concern.
- **10M cells per spreadsheet.** The add-on writes at most 10,000 rows per query.

#### Add-on constraints

- **Read-only SQL only.**
- **One non-`SET` statement per submit.** Leading `SET` statements are unrestricted.
- **SQL ≤ 65,536 characters.**
- **Current-spreadsheet scope only.** Cannot write to other spreadsheets.
- **Per-user credentials.** Each Google user configures their own AccessKey; no shared/team credentials, no SSO.
- **Local SQL history is browser-bound.** Not synced across browsers or accounts.
- **Not yet supported.** Multi-project switcher, query templates, CSV/Excel export, automated charts (on the roadmap).

### Supported Regions

**All MaxCompute regions are supported.** The add-on's `appsscript.json` ships with `urlFetchWhitelist` covering every official MaxCompute public endpoint worldwide. The Endpoint field accepts the standard form `https://service.{region}.maxcompute.aliyun.com/api` — just enter the endpoint of the region your project lives in.

### Documentation

| Document | Contents |
|----------|----------|
| [docs/read-only-sql-guard.md](docs/read-only-sql-guard.md) | Read-only SQL guard: algorithm, misclassifications, V1/V2 roadmap |
| [docs/long-running-jobs-instance-attach.md](docs/long-running-jobs-instance-attach.md) | Long-running jobs & attach guide |
| [docs/technical-design.md](docs/technical-design.md) | Architecture, signing, catalog, development & release workflow (for developers and maintainers) |

### License

[Apache License 2.0](LICENSE)
