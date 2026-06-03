# 只读 SQL 限制 / Read-Only SQL Guard

> 当前插件只允许提交只读查询。本文档说明这一限制**怎么实现**、**怎么演进**、**有哪些已知误判**。
> The add-on currently allows only read-only queries. This doc describes the implementation, the planned evolution, and the known misclassification cases.

[中文](#中文) | [English](#english)

---

## 中文

### 版本路线

| 版本 | 实现位置 | 特点 |
|------|----------|------|
| **V1（当前）** | 客户端（插件本身：浏览器侧栏 + Apps Script 后端） | 在请求到达 MaxCompute 之前用关键字 / 正则启发式拦截；只允许真正产生结果集的 DQL（`SELECT` 或 `WITH … SELECT`）；MaxCompute 服务端**不**做只读校验 |
| **V2（规划）** | MaxCompute 服务端 | 通过 MaxCompute RAM 角色 / 只读访问凭证 / 平台级查询权限隔离实现；插件层面的启发式可逐步降级为 UI 提示 |

V1 的本质是「信任客户端」：如果绕过插件、直接拿用户的 AccessKey 调 MaxCompute，本文描述的拦截不会生效——因此凭证按用户隔离、Endpoint 白名单、签名机制等其他防线同样关键。

V2 的目标是从根本上消除关键字启发式带来的误判问题：服务端权限裁决比关键字模式匹配更准、更稳，但它依赖 MaxCompute 平台能力或外部 RAM 策略支持，不在插件代码内可控。

### V1 实现位置

```
[浏览器侧栏 Sidebar.html]  ── 预检 ─→  快速给用户红字提示
            │
            │ google.script.run.submitQuery(sql, ...)
            ▼
[Apps Script 后端 SqlExecutor.assertReadOnlySql_()]
            │  ── 强制边界 ─→ 拒绝则不调用 UrlFetchApp.fetch()
            │
            │ HTTPS + ODPS V1 签名
            ▼
[MaxCompute 服务端]  ── 不做只读校验
```

| 层 | 位置 | 角色 |
|----|------|------|
| 浏览器预检 | `src/Sidebar.html` 中的 `getClientReadOnlySqlError()` | 仅作 UX 提示，提前红字反馈，**不**作为安全边界 |
| Apps Script 后端 | `src/SqlExecutor.js` 中的 `assertReadOnlySql_()` | 强制边界，在 `UrlFetchApp.fetch()` 之前执行；浏览器禁用 JS 也无法绕过 |

两层共享语句拆分、字面量遮蔽、关键字提取、黑白名单集合，只在错误提示文案上有差异。

### 算法

实现位于 `src/SqlExecutor.js`；浏览器预检 (`src/Sidebar.html → getClientReadOnlySqlError()`) 是它的 JS 镜像，仅错误文案不同。下面按调用顺序展示关键函数源码。

**输入**：用户输入的原始 SQL 字符串。
**输出**：通过 / 抛出 `Error`（含错误原因）。

#### 入口：`assertReadOnlySql_`

控制流：长度校验 → 切分为顶层语句 → 逐条提取首关键字 → 按关键字分类（SET / 允许 / 禁用 / 其他）→ 主查询数量约束。

```js
function assertReadOnlySql_(sql) {
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }
  assertSqlLength_(sql);

  var statements = splitSqlStatements_(sql);
  var hasMainQuery = false;

  for (var i = 0; i < statements.length; i++) {
    var statement = statements[i];
    var keyword = getFirstSqlKeyword_(statement);
    if (!keyword) {
      continue;
    }

    if (keyword === 'SET') {
      if (hasMainQuery) {
        throw new Error('SET 语句只能放在只读查询之前。');
      }
      if (containsReservedAuditSetStatement_(statement)) {
        throw new Error('不允许手动设置插件保留的 EXT_* 审计字段。');
      }
      continue;
    }

    if (hasMainQuery) {
      throw new Error('当前插件每次仅允许提交一条只读查询。');
    }

    if (isForbiddenSqlKeyword_(keyword) ||
        (shouldCheckNestedForbiddenSqlOperation_(keyword) && containsForbiddenSqlOperation_(statement))) {
      throw new Error(READ_ONLY_SQL_ERROR);
    }

    if (!isAllowedReadOnlySqlKeyword_(keyword)) {
      throw new Error('当前插件仅允许提交只读查询，不支持以 ' + keyword + ' 开头的 SQL。');
    }

    hasMainQuery = true;
  }

  if (!hasMainQuery) {
    throw new Error('当前插件仅允许提交 SELECT / WITH 只读查询。');
  }
}
```

#### 长度上限

```js
var MAX_USER_SQL_LENGTH = 64 * 1024;

function assertSqlLength_(sql) {
  if (String(sql || '').length > MAX_USER_SQL_LENGTH) {
    throw new Error('SQL 长度超过限制（最多 ' + MAX_USER_SQL_LENGTH + ' 字符）。');
  }
}
```

#### 顶层分号切分：`splitSqlStatements_`

按 `;` 切分语句，解析时识别字符串、反引号标识符、行注释、块注释，让其内部的分号不参与切分。

```js
function splitSqlStatements_(sql) {
  var statements = [];
  var current = [];
  var quote = null;
  var lineComment = false;
  var blockComment = false;

  for (var i = 0; i < sql.length; i++) {
    var ch = sql.charAt(i);
    var next = i + 1 < sql.length ? sql.charAt(i + 1) : '';

    if (lineComment) {
      if (ch === '\n' || ch === '\r') {
        lineComment = false;
        current.push(' ');
      }
      continue;
    }

    if (blockComment) {
      if (ch === '*' && next === '/') {
        blockComment = false;
        current.push(' ');
        i++;
      }
      continue;
    }

    if (quote) {
      current.push(ch);
      if (ch === '\\' && next) {
        current.push(next);
        i++;
      } else if (ch === quote) {
        if (next === quote) {
          current.push(next);
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '-' && next === '-') {
      lineComment = true;
      current.push(' ');
      i++;
      continue;
    }

    if (ch === '/' && next === '*') {
      blockComment = true;
      current.push(' ');
      i++;
      continue;
    }

    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      current.push(ch);
      continue;
    }

    if (ch === ';') {
      addSqlStatement_(statements, current.join(''));
      current = [];
      continue;
    }

    current.push(ch);
  }

  addSqlStatement_(statements, current.join(''));
  return statements;
}
```

#### 字面量遮蔽 + 首关键字提取

`maskSqlCommentsAndLiterals_` 用与 `splitSqlStatements_` 相同的状态机，但把字符串 / 反引号标识符 / 注释里的所有字符**替换为空白（保留原始长度）**，便于后续正则在原位定位（完整代码见 `src/SqlExecutor.js`）。

```js
function getFirstSqlKeyword_(statement) {
  var normalized = maskSqlCommentsAndLiterals_(statement)
    .replace(/^\s+/, '')
    .replace(/^[\s(]+/, '');
  var match = normalized.match(/^([A-Za-z_]+)/);
  return match ? match[1].toUpperCase() : '';
}
```

效果：`'INSERT' AS x` 这类字符串字面量、`` `commit` `` 反引号列名、行注释里的 `INSERT` 都会被遮蔽，因此既不会被当作首关键字，也不会被下面的内嵌副作用正则误命中。

#### 分类

```js
function isAllowedReadOnlySqlKeyword_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

function isForbiddenSqlKeyword_(keyword) {
  var forbidden = {
    INSERT: true, UPDATE: true, DELETE: true, MERGE: true,
    CREATE: true, ALTER: true, DROP: true, TRUNCATE: true,
    RENAME: true, GRANT: true, REVOKE: true,
    LOAD: true, UNLOAD: true, ANALYZE: true,
    CALL: true, USE: true,
    BEGIN: true, COMMIT: true, ROLLBACK: true
  };
  return !!forbidden[keyword];
}

function shouldCheckNestedForbiddenSqlOperation_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

function containsReservedAuditSetStatement_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  return /\bset\s+ext_(?:platform_id|node_id|dagtype|task_id|node_name|node_onduty)\b/i.test(s);
}
```

按首关键字 K：

- `K = SET`：保留作为 hint 通道（如 `SET odps.sql.mapper.split.size=512`）；必须出现在主查询之前。`SET EXT_*`（`EXT_PLATFORM_ID|EXT_NODE_ID|EXT_DAGTYPE|EXT_TASK_ID|EXT_NODE_NAME|EXT_NODE_ONDUTY`）保留给插件审计字段，详见 [docs/audit-metadata.md](audit-metadata.md)。
- `K ∈ { SELECT, WITH }`：进入下面的「内嵌副作用扫描」；通过即放行。
- `K ∈ 黑名单`：直接拒绝。
- `K = 任何其他标识符`（包括 `SHOW`、`DESC`、`DESCRIBE`、`EXPLAIN`）：直接拒绝；元数据查询请改用「Data Catalog」侧栏。

#### 内嵌副作用扫描

只对 `SELECT` / `WITH` 的语句正文跑——目的是拦截 `WITH … INSERT INTO …` 这类把副作用嵌进 CTE 里的绕过。所有正则在已遮蔽字面量/注释的语句正文上跑，大小写不敏感，`\b` 是 ASCII 单词边界。

```js
function containsForbiddenSqlOperation_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  var patterns = [
    /\binsert\s+(?:into|overwrite)\b/i,
    /\bupdate\s+[\s\S]+?\bset\b/i,
    /\bdelete\s+from\b/i,
    /\bmerge\s+into\b/i,
    /\bcreate\s+(?:or\s+replace\s+)?(?:external\s+)?(?:materialized\s+)?(?:table|view|function|resource|instance|schema|database|role|package|volume|model)\b/i,
    /\balter\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)\b/i,
    /\bdrop\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)\b/i,
    /\btruncate\s+table\b/i,
    /\brename\s+table\b/i,
    /\bmsck\s+repair\s+table\b/i,
    /\badd\s+(?:file|jar|archive|py|resource|user)\b/i,
    /\bremove\s+(?:file|jar|archive|py|resource|user)\b/i,
    /\b(?:install|uninstall)\s+package\b/i,
    /\bgrant\b/i,
    /\brevoke\b/i,
    /\bload\s+data\b/i,
    /\bunload\b/i,
    /\banalyze\s+(?:table|column|columns)\b/i,
    /\bcall\b/i,
    /\buse\s+\S+/i,
    /\bbegin\b/i,
    /\bcommit\b/i,
    /\brollback\b/i
  ];

  for (var i = 0; i < patterns.length; i++) {
    if (patterns[i].test(s)) {
      return true;
    }
  }
  return false;
}
```

#### 数量约束

由 `assertReadOnlySql_` 主循环维护的 `hasMainQuery` 标志保证：
- 至多一条非 `SET` 主查询；第二条会触发「当前插件每次仅允许提交一条只读查询」。
- 至少一条非 `SET` 主查询；仅 `SET` 会触发「当前插件仅允许提交 SELECT / WITH 只读查询」。

### 误判说明

#### 误拒（False Positive — 合法只读 SQL 被拒）

| 案例 | 是否会误拒 | 原因 |
|------|-----------|------|
| `SELECT 'INSERT' AS x;` | 否 | 字符串内容在算法第 3 步被遮蔽 |
| `` SELECT `commit` FROM t; `` | 否 | 反引号标识符被遮蔽 |
| `SELECT commit_id FROM t;` | 否 | `\bcommit\b` 不命中 `commit_id` |
| `SELECT commit FROM t;`（**无引号**列名 `commit`） | **会** | `\bcommit\b` 命中；解决：用反引号或重命名 |
| `SELECT call FROM t;`、`SELECT use FROM t;`、`SELECT begin FROM t;` 等 | **会** | 同上 |
| `WITH t AS (SELECT …) SELECT … FROM t;` | 否 | 主查询关键字 `WITH`，正文中无副作用模式 |

绕过误拒的实操方法：用反引号 `` `…` `` 包裹冲突的列名 / 标识符，例如 `` SELECT `commit` FROM t; ``。

#### 漏判（False Negative — 危险 SQL 被放行）

启发式黑名单不是 AST 解析，理论上仍可能：

- MaxCompute 引入新副作用关键字，黑名单未及时同步。
- 用稀有合法语法或方言绕过正则。**但**：MaxCompute 自己也会拒绝它不认识的语法，组合下来通常不会真的产生副作用。

V1 提供「插件自身职责范围内的尽力而为」，**真正的硬保证留给 V2 服务端权限隔离**。

#### 不归算法管的「拒绝」

下列拒绝**不是**算法限制，而是其它策略：

| 行为 | 实际原因 |
|------|---------|
| 拒绝 `SET EXT_PLATFORM_ID=...` 等 | 审计字段保留约定，详见 [docs/audit-metadata.md](audit-metadata.md) |
| 拒绝长度 > 65 536 字符的 SQL | 输入硬上限，与读写性质无关 |
| 同步路径超时 1–300 秒 | 执行模型，与只读校验无关 |

### 测试覆盖

`tests/local.test.js` 中以 `read_only_sql_*` 命名的用例覆盖了：

- 允许通过的 DQL 组合（`SELECT`、`WITH … SELECT`、前置 `SET`）
- 元数据 / 解释类首关键字的拒绝（`SHOW`、`DESC`、`DESCRIBE`、`EXPLAIN`，含 `EXPLAIN SELECT`、`EXPLAIN INSERT`、`EXPLAIN CREATE EXTERNAL/MATERIALIZED`、`EXPLAIN ADD FILE`、`EXPLAIN INSTALL PACKAGE`、`EXPLAIN MSCK REPAIR`）
- DDL / DML / 权限 / 加载首关键字的拒绝（`INSERT`、`UPDATE`、`DELETE`、`MERGE`、`CREATE`、`ALTER`、`DROP`、`TRUNCATE`、`GRANT`、`REVOKE`、`LOAD`、`UNLOAD`、`ANALYZE`、`CALL`、`USE`、`BEGIN` / `COMMIT` / `ROLLBACK`）
- `WITH … INSERT INTO …` 的内嵌副作用拦截
- `SET EXT_*` 保留字段拒绝
- 字面量 / 注释 / 反引号标识符遮蔽
- 多语句 / 空 SQL / 仅 SET 拒绝
- SQL 长度超限拒绝

---

## English

### Roadmap

| Version | Where the check runs | Notes |
|---------|----------------------|-------|
| **V1 (current)** | Client side — the add-on itself (browser sidebar + Apps Script backend) | Heuristic keyword/regex check before the request reaches MaxCompute; only true result-set DQL (`SELECT` or `WITH … SELECT`) is allowed. MaxCompute server **does not** enforce read-only. |
| **V2 (planned)** | MaxCompute server | Enforced via MaxCompute RAM role / read-only credential / platform-level query permission isolation. The plugin-level heuristic can degrade into a UX hint over time. |

V1 trusts the add-on. If someone bypasses the plugin and calls MaxCompute directly with the user's AccessKey, none of the rules in this document apply — which is why per-user credential storage, the endpoint allowlist, and request signing exist as separate defenses.

V2 eliminates heuristic misclassifications at the root: server-side authorization is more accurate than keyword matching, but it depends on MaxCompute platform features or external RAM policies that the plugin alone cannot deliver.

### V1 Layers

```
[Sidebar.html]  ── precheck ─→  fast inline error
       │
       │ google.script.run.submitQuery(sql, …)
       ▼
[SqlExecutor.assertReadOnlySql_()]
       │  ── enforced boundary ─→ rejects before any UrlFetchApp.fetch()
       │
       │ HTTPS + ODPS V1 signing
       ▼
[MaxCompute server]  ── no read-only check
```

| Layer | File | Role |
|-------|------|------|
| Browser precheck | `getClientReadOnlySqlError()` in `src/Sidebar.html` | UX only; gives an inline error fast. **Not** a security boundary; disabling JS bypasses it. |
| Apps Script backend | `assertReadOnlySql_()` in `src/SqlExecutor.js` | Enforced boundary; runs before `UrlFetchApp.fetch()`. Cannot be skipped from the browser. |

Both layers share the same statement splitting, literal masking, keyword extraction, and allow/deny lists — they differ only in error message wording.

### Algorithm

The implementation lives in `src/SqlExecutor.js`; the browser precheck (`src/Sidebar.html → getClientReadOnlySqlError()`) is a JS mirror of it, differing only in error wording. The walkthrough below follows the call order; source is pasted inline.

**Input**: the user-typed raw SQL.
**Output**: pass / throws an `Error` (with reason).

#### Entry point: `assertReadOnlySql_`

Control flow: length check → split into top-level statements → extract first keyword per statement → classify (SET / allow-list / deny-list / other) → cardinality constraint.

```js
function assertReadOnlySql_(sql) {
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }
  assertSqlLength_(sql);

  var statements = splitSqlStatements_(sql);
  var hasMainQuery = false;

  for (var i = 0; i < statements.length; i++) {
    var statement = statements[i];
    var keyword = getFirstSqlKeyword_(statement);
    if (!keyword) {
      continue;
    }

    if (keyword === 'SET') {
      if (hasMainQuery) {
        throw new Error('SET 语句只能放在只读查询之前。');
      }
      if (containsReservedAuditSetStatement_(statement)) {
        throw new Error('不允许手动设置插件保留的 EXT_* 审计字段。');
      }
      continue;
    }

    if (hasMainQuery) {
      throw new Error('当前插件每次仅允许提交一条只读查询。');
    }

    if (isForbiddenSqlKeyword_(keyword) ||
        (shouldCheckNestedForbiddenSqlOperation_(keyword) && containsForbiddenSqlOperation_(statement))) {
      throw new Error(READ_ONLY_SQL_ERROR);
    }

    if (!isAllowedReadOnlySqlKeyword_(keyword)) {
      throw new Error('当前插件仅允许提交只读查询，不支持以 ' + keyword + ' 开头的 SQL。');
    }

    hasMainQuery = true;
  }

  if (!hasMainQuery) {
    throw new Error('当前插件仅允许提交 SELECT / WITH 只读查询。');
  }
}
```

(Error messages are Chinese in the source; the browser precheck has English equivalents.)

#### Length cap

```js
var MAX_USER_SQL_LENGTH = 64 * 1024;

function assertSqlLength_(sql) {
  if (String(sql || '').length > MAX_USER_SQL_LENGTH) {
    throw new Error('SQL 长度超过限制（最多 ' + MAX_USER_SQL_LENGTH + ' 字符）。');
  }
}
```

#### Top-level split on `;`: `splitSqlStatements_`

Splits on `;` while a state machine recognises strings, backtick-quoted identifiers, line comments, and block comments — semicolons inside any of those don't split.

```js
function splitSqlStatements_(sql) {
  var statements = [];
  var current = [];
  var quote = null;
  var lineComment = false;
  var blockComment = false;

  for (var i = 0; i < sql.length; i++) {
    var ch = sql.charAt(i);
    var next = i + 1 < sql.length ? sql.charAt(i + 1) : '';

    if (lineComment) {
      if (ch === '\n' || ch === '\r') {
        lineComment = false;
        current.push(' ');
      }
      continue;
    }

    if (blockComment) {
      if (ch === '*' && next === '/') {
        blockComment = false;
        current.push(' ');
        i++;
      }
      continue;
    }

    if (quote) {
      current.push(ch);
      if (ch === '\\' && next) {
        current.push(next);
        i++;
      } else if (ch === quote) {
        if (next === quote) {
          current.push(next);
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '-' && next === '-') {
      lineComment = true;
      current.push(' ');
      i++;
      continue;
    }

    if (ch === '/' && next === '*') {
      blockComment = true;
      current.push(' ');
      i++;
      continue;
    }

    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      current.push(ch);
      continue;
    }

    if (ch === ';') {
      addSqlStatement_(statements, current.join(''));
      current = [];
      continue;
    }

    current.push(ch);
  }

  addSqlStatement_(statements, current.join(''));
  return statements;
}
```

#### Literal masking + first-keyword extraction

`maskSqlCommentsAndLiterals_` runs the same state machine as `splitSqlStatements_`, but replaces every character inside strings / backtick identifiers / comments with whitespace **(preserving offsets)**, so later regex scans stay positionally correct. Full code in `src/SqlExecutor.js`.

```js
function getFirstSqlKeyword_(statement) {
  var normalized = maskSqlCommentsAndLiterals_(statement)
    .replace(/^\s+/, '')
    .replace(/^[\s(]+/, '');
  var match = normalized.match(/^([A-Za-z_]+)/);
  return match ? match[1].toUpperCase() : '';
}
```

Effect: `'INSERT' AS x` literals, `` `commit` `` backtick column names, and `INSERT` inside line comments are all blanked out, so they cannot become the first keyword and cannot trigger the nested side-effect regex below.

#### Classification

```js
function isAllowedReadOnlySqlKeyword_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

function isForbiddenSqlKeyword_(keyword) {
  var forbidden = {
    INSERT: true, UPDATE: true, DELETE: true, MERGE: true,
    CREATE: true, ALTER: true, DROP: true, TRUNCATE: true,
    RENAME: true, GRANT: true, REVOKE: true,
    LOAD: true, UNLOAD: true, ANALYZE: true,
    CALL: true, USE: true,
    BEGIN: true, COMMIT: true, ROLLBACK: true
  };
  return !!forbidden[keyword];
}

function shouldCheckNestedForbiddenSqlOperation_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

function containsReservedAuditSetStatement_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  return /\bset\s+ext_(?:platform_id|node_id|dagtype|task_id|node_name|node_onduty)\b/i.test(s);
}
```

Per first keyword K:

- `K = SET`: kept as a hint channel (e.g. `SET odps.sql.mapper.split.size=512`); must appear before the main query. `SET EXT_*` (`EXT_PLATFORM_ID|EXT_NODE_ID|EXT_DAGTYPE|EXT_TASK_ID|EXT_NODE_NAME|EXT_NODE_ONDUTY`) is reserved for the add-on's audit injection — see [docs/audit-metadata.md](audit-metadata.md).
- `K ∈ { SELECT, WITH }`: enters the nested side-effect scan below; passes if no pattern matches.
- `K ∈ deny-list`: rejected.
- `K = anything else` (including `SHOW`, `DESC`, `DESCRIBE`, `EXPLAIN`): rejected. For metadata, use the *Data Catalog* sidebar instead.

#### Nested side-effect scan

Runs only on the body of `SELECT` / `WITH` statements; this is what catches `WITH … INSERT INTO …` and similar attempts to hide a side effect inside a CTE. All patterns run on the masked statement body, are case-insensitive, and `\b` is the ASCII word boundary.

```js
function containsForbiddenSqlOperation_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  var patterns = [
    /\binsert\s+(?:into|overwrite)\b/i,
    /\bupdate\s+[\s\S]+?\bset\b/i,
    /\bdelete\s+from\b/i,
    /\bmerge\s+into\b/i,
    /\bcreate\s+(?:or\s+replace\s+)?(?:external\s+)?(?:materialized\s+)?(?:table|view|function|resource|instance|schema|database|role|package|volume|model)\b/i,
    /\balter\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)\b/i,
    /\bdrop\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)\b/i,
    /\btruncate\s+table\b/i,
    /\brename\s+table\b/i,
    /\bmsck\s+repair\s+table\b/i,
    /\badd\s+(?:file|jar|archive|py|resource|user)\b/i,
    /\bremove\s+(?:file|jar|archive|py|resource|user)\b/i,
    /\b(?:install|uninstall)\s+package\b/i,
    /\bgrant\b/i,
    /\brevoke\b/i,
    /\bload\s+data\b/i,
    /\bunload\b/i,
    /\banalyze\s+(?:table|column|columns)\b/i,
    /\bcall\b/i,
    /\buse\s+\S+/i,
    /\bbegin\b/i,
    /\bcommit\b/i,
    /\brollback\b/i
  ];

  for (var i = 0; i < patterns.length; i++) {
    if (patterns[i].test(s)) {
      return true;
    }
  }
  return false;
}
```

#### Cardinality constraint

The `hasMainQuery` flag in the main `assertReadOnlySql_` loop enforces:
- At most one non-`SET` main query — a second one throws "Only one read-only query is allowed per submission."
- At least one non-`SET` main query — a SET-only submission throws "Only `SELECT` / `WITH` read-only queries are accepted."

### Misclassifications

#### False positives (legitimate read-only SQL rejected)

| Case | Rejected? | Reason |
|------|-----------|--------|
| `SELECT 'INSERT' AS x;` | No | String content masked in step 3. |
| `` SELECT `commit` FROM t; `` | No | Backtick-quoted identifier masked. |
| `SELECT commit_id FROM t;` | No | `\bcommit\b` does not match `commit_id`. |
| `SELECT commit FROM t;` (**unquoted** column named `commit`) | **Yes** | `\bcommit\b` matches; quote the column or rename it. |
| `SELECT call FROM t;`, `SELECT use FROM t;`, `SELECT begin FROM t;`, etc. | **Yes** | Same as above. |
| `WITH t AS (SELECT …) SELECT … FROM t;` | No | First keyword is `WITH`; body has no side-effect substring. |

Workaround for false positives: backtick-quote the conflicting identifier, e.g. `` SELECT `commit` FROM t; ``.

#### False negatives (dangerous SQL allowed)

Heuristic deny lists are not AST parsing, so in theory:

- MaxCompute introduces a new side-effect keyword that we have not added to the list.
- An obscure dialectal form bypasses the regex set. **However**: MaxCompute itself rejects syntax it does not recognise, so an actual side-effect rarely materialises.

V1 is the add-on's best-effort protection. **Hard guarantees belong to V2 server-side authorization.**

#### Not the algorithm's job

These rejections are unrelated to the read-only check:

| Behaviour | Reason |
|-----------|--------|
| Reject `SET EXT_PLATFORM_ID=...` etc. | Audit reservation — see [docs/audit-metadata.md](audit-metadata.md). |
| Reject SQL longer than 65,536 chars | Input length cap, independent of read/write. |
| Synchronous-path timeout 1–300 s | Execution model, not the read-only check. |

### Test coverage

The `read_only_sql_*` tests in `tests/local.test.js` cover:

- Allowed DQL combinations (`SELECT`, `WITH … SELECT`, leading `SET`).
- Rejection of metadata / explanatory first keywords (`SHOW`, `DESC`, `DESCRIBE`, `EXPLAIN`, including `EXPLAIN SELECT`, `EXPLAIN INSERT`, `EXPLAIN CREATE EXTERNAL/MATERIALIZED`, `EXPLAIN ADD FILE`, `EXPLAIN INSTALL PACKAGE`, `EXPLAIN MSCK REPAIR`).
- Rejection of denied first keywords (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `GRANT`, `REVOKE`, `LOAD`, `UNLOAD`, `ANALYZE`, `CALL`, `USE`, `BEGIN` / `COMMIT` / `ROLLBACK`).
- Nested side-effect detection: `WITH … INSERT INTO …`.
- Reserved `SET EXT_*` rejection.
- Literal / comment / backtick-identifier masking.
- Multi-statement, empty SQL, and SET-only rejection.
- Length-cap rejection.
