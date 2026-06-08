// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * MaxCompute SQL 执行器
 *
 * 使用 MaxCompute Instance Job API 执行 SQL 查询。
 * 支持同步和异步执行模式。
 */

// ============================================================
// 常量
// ============================================================

/** 默认任务名称 */
var SQL_TASK_NAME = 'query_task';

/** 轮询间隔序列（毫秒）- 固定递增：1s, 2s, 4s, 8s */
var POLL_INTERVALS = [1000, 2000, 4000, 8000];

/** SQL 轮询超时默认值（秒） */
var DEFAULT_SQL_TIMEOUT_SECONDS = 300;

/** SQL 同步轮询超时上限（秒），避免单个 Apps Script 调用超过平台限制 */
var MAX_SQL_TIMEOUT_SECONDS = 300;

/** SQL 同步轮询超时下限（秒），避免无效输入导致立即异常轮询 */
var MIN_SQL_TIMEOUT_SECONDS = 1;

/** GSheet 插件提交到 MaxCompute 的平台标识（MaxCompute EXT_PLATFORM_ID 限制 32 字符） */
var AUDIT_PLATFORM_ID = 'Gsheet';

/** 只读 SQL 策略错误提示 */
var READ_ONLY_SQL_ERROR = '当前插件仅允许提交只读查询，不支持 DDL/DML、权限、加载或其他副作用语句。';

/** 用户 SQL 最大长度，避免异常大输入拖慢 Apps Script 或污染请求体 */
var MAX_USER_SQL_LENGTH = 64 * 1024;

/** Instance ID 最大长度兜底，防止异常输入污染日志或请求路径 */
var MAX_INSTANCE_ID_LENGTH = 128;

/**
 * 解析超时参数，返回毫秒数
 * @param {number|string} timeoutSeconds - 来自调用方的超时（秒），无效时使用默认值
 */
function resolveTimeoutMs_(timeoutSeconds) {
  var n = parseInt(timeoutSeconds, 10);
  if (isNaN(n) || n <= 0) {
    n = DEFAULT_SQL_TIMEOUT_SECONDS;
  }
  n = Math.max(MIN_SQL_TIMEOUT_SECONDS, Math.min(n, MAX_SQL_TIMEOUT_SECONDS));
  return n * 1000;
}

/**
 * 获取当前提交作业用户的 Google 邮箱，用于 MaxCompute 作业审计。
 */
function getCurrentUserAuditKey_() {
  var email = '';
  try {
    email = Session.getActiveUser().getEmail();
    if (email) return email;
  } catch (e) {}

  try {
    email = getCurrentUserEmailFromUserInfo_();
    if (email) return email;
  } catch (e2) {}

  return 'unknown';
}

/**
 * 通过当前 OAuth token 调 userinfo 兜底获取邮箱。
 *
 * Apps Script 的 Session.getActiveUser().getEmail() 在部分执行/发布上下文会返回空；
 * manifest 已声明 userinfo.email，因此这里用同一授权范围补齐提交人审计字段。
 *
 * @return {string}
 */
function getCurrentUserEmailFromUserInfo_() {
  var token = ScriptApp.getOAuthToken();
  if (!token) {
    return '';
  }

  var response = UrlFetchApp.fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
    method: 'get',
    headers: {
      Authorization: 'Bearer ' + token,
      'User-Agent': MC_GSHEET_PLUGIN_USER_AGENT
    },
    muteHttpExceptions: true
  });

  if (response.getResponseCode() !== 200) {
    Logger.log('[SqlExecutor] userinfo email fallback HTTP ' + response.getResponseCode());
    return '';
  }

  var data = JSON.parse(response.getContentText() || '{}');
  return data && data.email ? String(data.email) : '';
}

/**
 * 校验用户 SQL 是否为只读查询。
 *
 * 策略：
 * - 允许查询前有若干 SET 语句
 * - 允许一条 SELECT / WITH（即真正产生结果集的 DQL）
 * - 禁止 DDL/DML/权限/加载/元数据/EXPLAIN 等非 DQL 语句
 * - 禁止多条非 SET 语句
 *
 * @param {string} sql - 用户原始 SQL（不包含系统审计 SET）
 * @throws {Error} 非只读 SQL 时抛错
 */
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

/**
 * 校验用户 SQL 长度。
 *
 * @param {string} sql
 * @throws {Error} SQL 过长时抛错
 */
function assertSqlLength_(sql) {
  if (String(sql || '').length > MAX_USER_SQL_LENGTH) {
    throw new Error('SQL 长度超过限制（最多 ' + MAX_USER_SQL_LENGTH + ' 字符）。');
  }
}

/**
 * 将 SQL 按顶层分号切分，忽略字符串、反引号标识符、行注释和块注释中的分号。
 *
 * @param {string} sql
 * @return {string[]}
 */
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

/**
 * 添加非空 SQL 片段。
 *
 * @param {string[]} statements
 * @param {string} statement
 */
function addSqlStatement_(statements, statement) {
  statement = (statement || '').replace(/^\s+|\s+$/g, '');
  if (statement) {
    statements.push(statement);
  }
}

/**
 * 获取 SQL 语句第一个关键字。
 *
 * @param {string} statement
 * @return {string}
 */
function getFirstSqlKeyword_(statement) {
  var normalized = maskSqlCommentsAndLiterals_(statement)
    .replace(/^\s+/, '')
    .replace(/^[\s(]+/, '');
  var match = normalized.match(/^([A-Za-z_]+)/);
  return match ? match[1].toUpperCase() : '';
}

/**
 * 校验并规范化 Instance ID。
 *
 * @param {string} instanceId
 * @return {string}
 */
function normalizeInstanceId_(instanceId) {
  instanceId = String(instanceId || '').trim();
  if (!instanceId) {
    throw new Error('instanceId 不能为空');
  }
  if (instanceId.length > MAX_INSTANCE_ID_LENGTH ||
      !/^[A-Za-z0-9_.:-]+$/.test(instanceId) ||
      instanceId.indexOf('..') !== -1) {
    throw new Error('Instance ID 格式不正确');
  }
  return instanceId;
}

/**
 * 判断开头关键字是否是允许的只读语句。
 *
 * @param {string} keyword
 * @return {boolean}
 */
function isAllowedReadOnlySqlKeyword_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

/**
 * 判断开头关键字是否明确属于禁用语句。
 *
 * @param {string} keyword
 * @return {boolean}
 */
function isForbiddenSqlKeyword_(keyword) {
  var forbidden = {
    INSERT: true,
    UPDATE: true,
    DELETE: true,
    MERGE: true,
    CREATE: true,
    ALTER: true,
    DROP: true,
    TRUNCATE: true,
    RENAME: true,
    GRANT: true,
    REVOKE: true,
    LOAD: true,
    UNLOAD: true,
    ANALYZE: true,
    CALL: true,
    USE: true,
    BEGIN: true,
    COMMIT: true,
    ROLLBACK: true
  };
  return !!forbidden[keyword];
}

/**
 * 禁止用户手动设置插件保留的 MaxCompute 审计字段，避免覆盖自动注入的来源标识。
 *
 * @param {string} statement
 * @return {boolean}
 */
function containsReservedAuditSetStatement_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  return /set\s+ext_(?:platform_id|node_id|dagtype|task_id|node_name|node_onduty)/i.test(s);
}

/**
 * SELECT / WITH 可能包裹 DML 语句（如 `WITH ... INSERT`），需要继续扫描。
 */
function shouldCheckNestedForbiddenSqlOperation_(keyword) {
  return keyword === 'SELECT' || keyword === 'WITH';
}

/**
 * 在允许以 SELECT / WITH 开头的语句中继续排查隐藏的 DDL/DML。
 *
 * @param {string} statement
 * @return {boolean}
 */
function containsForbiddenSqlOperation_(statement) {
  var s = maskSqlCommentsAndLiterals_(statement);
  var patterns = [
    /insert\s+(?:into|overwrite)/i,
    /update\s+[\s\S]+?set/i,
    /delete\s+from/i,
    /merge\s+into/i,
    /create\s+(?:or\s+replace\s+)?(?:external\s+)?(?:materialized\s+)?(?:table|view|function|resource|instance|schema|database|role|package|volume|model)/i,
    /alter\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)/i,
    /drop\s+(?:materialized\s+)?(?:table|view|function|resource|schema|database|role|package|volume|model)/i,
    /truncate\s+table/i,
    /rename\s+table/i,
    /msck\s+repair\s+table/i,
    /add\s+(?:file|jar|archive|py|resource|user)/i,
    /remove\s+(?:file|jar|archive|py|resource|user)/i,
    /(?:install|uninstall)\s+package/i,
    /grant/i,
    /revoke/i,
    /load\s+data/i,
    /unload/i,
    /analyze\s+(?:table|column|columns)/i,
    /call/i,
    /use\s+\S+/i,
    /begin/i,
    /commit/i,
    /rollback/i
  ];

  for (var i = 0; i < patterns.length; i++) {
    if (patterns[i].test(s)) {
      return true;
    }
  }
  return false;
}

/**
 * 将注释、字符串字面量和引号标识符替换为空白，便于策略正则检查。
 *
 * @param {string} sql
 * @return {string}
 */
function maskSqlCommentsAndLiterals_(sql) {
  var out = [];
  var quote = null;
  var lineComment = false;
  var blockComment = false;

  for (var i = 0; i < sql.length; i++) {
    var ch = sql.charAt(i);
    var next = i + 1 < sql.length ? sql.charAt(i + 1) : '';

    if (lineComment) {
      if (ch === '\n' || ch === '\r') {
        lineComment = false;
        out.push(ch);
      } else {
        out.push(' ');
      }
      continue;
    }

    if (blockComment) {
      if (ch === '*' && next === '/') {
        blockComment = false;
        out.push('  ');
        i++;
      } else {
        out.push(' ');
      }
      continue;
    }

    if (quote) {
      out.push(' ');
      if (ch === '\\' && next) {
        out.push(' ');
        i++;
      } else if (ch === quote) {
        if (next === quote) {
          out.push(' ');
          i++;
        } else {
          quote = null;
        }
      }
      continue;
    }

    if (ch === '-' && next === '-') {
      lineComment = true;
      out.push('  ');
      i++;
      continue;
    }

    if (ch === '/' && next === '*') {
      blockComment = true;
      out.push('  ');
      i++;
      continue;
    }

    if (ch === '\'' || ch === '"' || ch === '`') {
      quote = ch;
      out.push(' ');
      continue;
    }

    out.push(ch);
  }

  return out.join('');
}


// ============================================================
// 公开接口
// ============================================================

/**
 * 执行 SQL 查询（高层封装）
 *
 * @param {string} sql - SQL 语句
 * @param {number} [timeoutSeconds] - 轮询超时（秒），缺省使用默认值
 * @return {Object} { columns: string[], rows: string[][], rowCount: number, instanceId: string }
 * @throws {Error} SQL 执行失败或超时
 */
function executeSqlQuery_(sql, timeoutSeconds, auditContext) {
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }

  var maxPollMs = resolveTimeoutMs_(timeoutSeconds);
  Logger.log('[SqlExecutor] user=' + getCurrentUserAuditKey_() +
    ' timeoutSec=' + Math.round(maxPollMs / 1000) +
    ' sql=' + sql);

  // 1. 提交作业
  var submitResult = submitSqlJob_(sql.trim(), auditContext);

  // 2. 如果是同步执行，直接返回结果
  if (submitResult.sync) {
    Logger.log('[SqlExecutor] 同步执行完成');
    return submitResult.result;
  }

  // 3. 异步轮询状态（固定递增间隔：1s, 2s, 4s, 8s）
  var instanceId = submitResult.instanceId;
  Logger.log('[SqlExecutor] 异步作业: instanceId=' + instanceId);

  // 3.1 等待 Instance 终止
  var startTime = Date.now();
  var pollIndex = 0;

  while (Date.now() - startTime < maxPollMs) {
    var status = getJobStatus_(instanceId);

    if (status.status === 'Terminated') {
      break;
    }

    // 固定递增间隔：1s, 2s, 4s, 8s，之后保持 8s
    var interval = POLL_INTERVALS[Math.min(pollIndex, POLL_INTERVALS.length - 1)];
    pollIndex++;
    Utilities.sleep(interval);
  }

  // 3.2 检查是否超时（不自动 KILL，避免长查询被强制终止）
  var finalStatus = getJobStatus_(instanceId);
  if (finalStatus.status !== 'Terminated') {
    var timeoutSec = Math.round(maxPollMs / 1000);
    throw new Error('SQL 执行超时（超过 ' + timeoutSec + ' 秒），instanceId=' + instanceId);
  }

  // 3.3 获取 Task 状态（参考 Java SDK: GET ?taskstatus）
  var taskInfo = getTaskStatus_(instanceId);
  var taskStatus = taskInfo.taskStatus;

  Logger.log('[SqlExecutor] Task 状态: ' + taskStatus);

  // 3.4 根据 Task 状态处理结果（参考 Java SDK checkTaskFailed）
  if (taskStatus === 'Success') {
    var result = getJobResult_(instanceId);
    result.instanceId = instanceId;
    return result;
  } else if (taskStatus === 'Failed') {
    throw new Error('SQL 执行失败: ' + getJobFailureSummary_(instanceId) +
      ' (instanceId=' + instanceId + ')');
  } else {
    // 其他状态（Cancelled, Suspended 等）
    throw new Error('SQL 任务状态异常: ' + taskStatus + ' (instanceId=' + instanceId + ')');
  }
}

/**
 * 只提交 SQL 作业，不等待结果
 * 用于前端显示 instance id
 *
 * @param {string} sql - SQL 语句
 * @return {Object} { instanceId: string } 或 { sync: true, result: Object } (同步执行)
 * @throws {Error} 提交失败
 */
function submitSqlJobOnly_(sql, auditContext) {
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }

  Logger.log('[SqlExecutor] submitSqlJobOnly user=' + getCurrentUserAuditKey_() +
    ' sql=' + sql);

  var submitResult = submitSqlJob_(sql.trim(), auditContext);

  if (submitResult.sync) {
    // 同步执行完成，直接返回结果
    return {
      sync: true,
      result: submitResult.result
    };
  }

  return {
    instanceId: submitResult.instanceId
  };
}

/**
 * 查询作业进度（前端轮询使用，单次调用，不阻塞）
 *
 * 工作流程：
 *   1. 查询 Instance 状态
 *   2. 若 Terminated，再查询 Task 状态
 *   3. 若 Task 失败，附带失败原因
 *
 * @param {string} instanceId - 实例 ID
 * @return {Object} {
 *   instanceTerminated: boolean,
 *   instanceStatus: string,        // Running | Suspended | Terminated
 *   taskStatus?: string,           // Waiting | Running | Success | Failed | Cancelled
 *   errorSummary?: string          // taskStatus === 'Failed' 时的安全失败摘要
 * }
 */
function getJobProgress_(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);

  var instanceStatus = getJobStatus_(instanceId).status;
  if (instanceStatus !== 'Terminated') {
    return {
      instanceTerminated: false,
      instanceStatus: instanceStatus
    };
  }

  var taskInfo = getTaskStatus_(instanceId);
  var taskStatus = taskInfo.taskStatus;

  var result = {
    instanceTerminated: true,
    instanceStatus: instanceStatus,
    taskStatus: taskStatus
  };

  if (taskStatus === 'Failed') {
    result.errorSummary = getJobFailureSummary_(instanceId);
  }

  return result;
}

/**
 * 获取作业失败摘要。返回 MaxCompute 服务端返回的失败码与消息原文。
 *
 * @param {string} instanceId
 * @return {string}
 */
function getJobFailureSummary_(instanceId) {
  try {
    var errorResult = getJobResult_(instanceId);
    return summarizeJobFailureResult_(errorResult.rawResult || '');
  } catch (e) {
    return 'fetchFailureReasonFailed: ' +
      String(e && e.message ? e.message : e || '');
  }
}

/**
 * 生成作业失败摘要：XML 错误体走 parseErrorSummary_ 取出 Code + Message，
 * 其他文本原样返回，供日志和前端展示。
 *
 * @param {string} rawResult
 * @return {string}
 */
function summarizeJobFailureResult_(rawResult) {
  rawResult = String(rawResult || '');
  if (!rawResult) {
    return '';
  }
  if (/^\s*</.test(rawResult)) {
    return parseErrorSummary_(rawResult);
  }
  return rawResult;
}


// ============================================================
// SQL 作业操作
// ============================================================

/**
 * 提交 SQL 作业
 *
 * @param {string} sql - SQL 语句
 * @return {Object} { sync: boolean, instanceId?: string, result?: Object }
 * @throws {Error} 提交失败
 */
function submitSqlJob_(sql, auditContext) {
  assertReadOnlySql_(sql);

  var config = getMcConfig_();
  assertUsableMcConfig_(config);
  var taskName = buildAuditTaskName_(auditContext);
  var auditSettings = buildAuditSettings_(auditContext);
  var parsedSql = SettingsParser_.parse(sql);
  var taskSettings = mergeSqlTaskSettings_(parsedSql.settings, auditSettings);
  var body = buildSqlJobXml_(parsedSql.sql, taskName, taskSettings);

  var response = odpsFetch_({
    method: 'POST',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'instances']),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    body: body
  });

  var code = response.getResponseCode();
  var responseText = response.getContentText();

  Logger.log('[SqlExecutor] submitSqlJob HTTP ' + code + getOdpsRequestIdLogSuffix_(response));
  if (code === 400) {
    logSubmitSqlJobBadRequestBody_(body);
  }

  if (code === 200) {
    // 同步执行成功
    return {
      sync: true,
      result: parseInstanceResultXmlSafe_(responseText)
    };
  } else if (code === 201) {
    // 异步作业创建成功，从 Location header 获取 instanceId
    var headers = response.getHeaders();
    var location = headers['Location'] || headers['location'] || '';
    var instanceId = normalizeInstanceId_(location.split('/').pop());
    return {
      sync: false,
      instanceId: instanceId
    };
  } else {
    var errMsg = parseErrorSummary_(responseText);
    throw new Error('提交作业失败 (HTTP ' + code + ')' + getOdpsRequestIdLogSuffix_(response) + ': ' + errMsg);
  }
}

/**
 * HTTP 400 通常表示提交 XML/SQL 内容无法被服务端解析。按诊断需要记录完整
 * Instance 请求体；该日志可能包含用户 SQL 和审计 SET 字段，只在 400 时输出。
 *
 * @param {string} body
 */
function logSubmitSqlJobBadRequestBody_(body) {
  Logger.log('[SqlExecutor] submitSqlJob HTTP 400 requestBody=' + String(body || ''));
}

/**
 * 查询作业状态（Instance 级别）
 *
 * @param {string} instanceId - 实例 ID
 * @return {Object} { status: string } - status: Running | Suspended | Terminated
 */
function getJobStatus_(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'instances', instanceId]),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throw new Error('查询状态失败 (HTTP ' + code + ')' + getOdpsRequestIdLogSuffix_(response));
  }

  return parseInstanceStatusXml_(response.getContentText());
}

/**
 * 查询 Task 状态
 * 参考 Java SDK: GET /instances/{id}?taskstatus
 *
 * @param {string} instanceId - 实例 ID
 * @return {Object} { taskName: string, taskStatus: string } - taskStatus: Waiting | Running | Success | Failed | Cancelled
 */
function getTaskStatus_(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'instances', instanceId]),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: { taskstatus: '' }
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throw new Error('查询 Task 状态失败 (HTTP ' + code + ')' + getOdpsRequestIdLogSuffix_(response));
  }

  return parseTaskStatusXml_(response.getContentText());
}

/**
 * 获取作业结果
 *
 * @param {string} instanceId - 实例 ID
 * @return {Object} { columns: string[], rows: string[][], rowCount: number }
 */
function getJobResult_(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'instances', instanceId]),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: { result: '' }  // 空值表示获取结果
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throw new Error('获取结果失败 (HTTP ' + code + ')' + getOdpsRequestIdLogSuffix_(response));
  }

  return parseInstanceResultXmlSafe_(response.getContentText());
}

/**
 * 终止 Instance（best-effort）
 * 参考 Java SDK Instance.stop():
 *   PUT /projects/{project}/instances/{id}
 *   <Instance><Status>Terminated</Status></Instance>
 *
 * @param {string} instanceId - 实例 ID
 * @param {string} [reason]   - 触发原因，仅用于日志（client_timeout / user_cancel）
 * @return {string} 结果摘要：'ok' | 'already_terminated' | 'failed:<msg>'
 */
function killInstance_(instanceId, reason) {
  instanceId = normalizeInstanceId_(instanceId);
  reason = reason || 'unknown';
  var startMs = Date.now();
  Logger.log('[SqlExecutor] kill instanceId=' + instanceId + ' reason=' + reason);

  try {
    var config = getMcConfig_();
    assertUsableMcConfig_(config);
    var body = '<Instance><Status>Terminated</Status></Instance>';

    var response = odpsFetch_({
      method: 'PUT',
      host: config.endpoint,
      pathname: buildOdpsPath_(['projects', config.project, 'instances', instanceId]),
      accessKeyId: config.accessKeyId,
      accessKeySecret: config.accessKeySecret,
      securityToken: config.securityToken || null,
      project: config.project,
      body: body
    });

    var code = response.getResponseCode();
    var elapsed = Date.now() - startMs;

    if (code >= 200 && code < 300) {
      Logger.log('[SqlExecutor] kill instanceId=' + instanceId + ' result=ok HTTP=' + code + ' elapsedMs=' + elapsed);
      return 'ok';
    }

    // 服务端可能因为已终止而返回 4xx —— 视为幂等成功
    var errText = response.getContentText() || '';
    if (/Terminated|InvalidStateTransition|InstanceFinished/i.test(errText)) {
      Logger.log('[SqlExecutor] kill instanceId=' + instanceId + ' result=already_terminated HTTP=' + code);
      return 'already_terminated';
    }

    var snippet = parseErrorSummary_(errText);
    Logger.log('[SqlExecutor] kill instanceId=' + instanceId + ' result=failed HTTP=' + code + ' err=' + snippet);
    return 'failed:HTTP_' + code;
  } catch (e) {
    var errMessage = String(e && e.message ? e.message : e || '');
    Logger.log('[SqlExecutor] kill instanceId=' + instanceId + ' result=exception err=' + errMessage);
    return 'failed:exception:' + errMessage;
  }
}

/**
 * 公开接口：取消正在运行的查询（供 Sidebar 手动 Cancel 使用）
 *
 * @param {string} instanceId
 * @return {string} 'ok' | 'already_terminated' | 'failed:<msg>'
 */
function cancelSqlJob_(instanceId) {
  return killInstance_(instanceId, 'user_cancel');
}


// ============================================================
// XML 构建
// ============================================================

/**
 * 构建 MaxCompute 通用作业标识 Settings。
 *
 * 这些 EXT_* 字段作为 SQLTask settings property 提交，避免拼到 Query 里被
 * SQL parser 当作普通 SET 语句解析。
 *
 * @param {Object} auditContext - { user, spreadsheetName, spreadsheetId, targetSheet }
 * @return {Object} settings map
 */
function buildAuditSettings_(auditContext) {
  var settings = {};
  if (!auditContext) {
    return settings;
  }

  var spreadsheetName = auditContext.spreadsheetName || '';
  var spreadsheetId = auditContext.spreadsheetId || 'unknown_sheet_id';
  var targetSheet = auditContext.targetSheet || '';
  var user = auditContext.user || getCurrentUserAuditKey_();

  settings.EXT_PLATFORM_ID = truncateForAudit_(AUDIT_PLATFORM_ID, 32);
  settings.EXT_NODE_ID = truncateForAudit_(buildAuditNodeId_(spreadsheetId), 64);

  if (spreadsheetName) {
    settings.EXT_NODE_NAME = truncateForAudit_(normalizeAuditValue_(spreadsheetName), 128);
  }

  if (targetSheet) {
    settings.EXT_TASK_ID = truncateForAudit_(normalizeAuditValue_(targetSheet), 64);
  }

  settings.EXT_NODE_ONDUTY = truncateForAudit_(normalizeAuditValue_(user), 64);

  return settings;
}

/**
 * 构建节点 ID，使用 Google Spreadsheet ID 并按 EXT_NODE_ID 限制裁剪。
 *
 * @param {string} spreadsheetId
 * @return {string}
 */
function buildAuditNodeId_(spreadsheetId) {
  return truncateForAudit_(spreadsheetId || 'unknown_sheet_id', 64);
}

/**
 * 将 GSheet 上下文压缩进 Task Name，便于直接在 Instance Task 信息中识别来源。
 *
 * @param {Object} auditContext
 * @return {string}
 */
function buildAuditTaskName_(auditContext) {
  if (!auditContext) {
    return SQL_TASK_NAME;
  }

  var targetSheet = auditContext.targetSheet || 'Query Result';
  var suffix = normalizeTaskNamePart_(targetSheet);
  return truncateForAudit_(SQL_TASK_NAME + '_' + suffix, 64);
}

/**
 * 清理审计字段中的控制空白，避免文件名/表单名里的换行影响 SQL 可读性。
 *
 * @param {string} value
 * @return {string}
 */
function normalizeAuditValue_(value) {
  return String(value || '').replace(/[\r\n\t]+/g, ' ');
}

/**
 * SQL 字符串字面量转义。
 *
 * @param {string} value
 * @return {string}
 */
function truncateForAudit_(value, maxLen) {
  value = normalizeAuditValue_(value);
  if (value.length <= maxLen) {
    return value;
  }
  return value.substring(0, maxLen);
}

/**
 * 将表单名规范化为适合 Task Name 的短字符串。
 *
 * @param {string} value
 * @return {string}
 */
function normalizeTaskNamePart_(value) {
  value = String(value || 'sheet').replace(/[^A-Za-z0-9_]+/g, '_');
  value = value.replace(/^_+|_+$/g, '');
  return value || 'sheet';
}

/**
 * 构建 SQL 作业请求体（XML）
 *
 * @param {string} sql - SQL 语句
 * @param {string} taskName - 任务名称
 * @param {Object} settings - SQLTask settings hints
 * @return {string} XML 字符串
 */
function buildSqlJobXml_(sql, taskName, settings) {
  return '<Instance>' +
    '<Job>' +
      '<Priority>5</Priority>' +
      '<Tasks>' +
        '<SQL>' +
          '<Name>' + escapeXml_(taskName) + '</Name>' +
          buildTaskConfigXml_(settings) +
          '<Query>' + escapeXml_(sql) + '</Query>' +
        '</SQL>' +
      '</Tasks>' +
    '</Job>' +
  '</Instance>';
}

/**
 * 构建 Task Config。MaxCompute Java SDK 的 SQLTask hints 会被序列化为
 * Config/Property: name=settings, value=<JSON>。
 *
 * @param {Object} settings
 * @return {string}
 */
function buildTaskConfigXml_(settings) {
  settings = settings || {};
  if (Object.keys(settings).length === 0) {
    return '';
  }

  return '<Config>' +
    '<Property>' +
      '<Name>settings</Name>' +
      '<Value>' + escapeXml_(JSON.stringify(settings)) + '</Value>' +
    '</Property>' +
  '</Config>';
}

/**
 * 合并用户 SET 解析出的 hints 和插件自动审计 hints。自动审计字段后写入，
 * 即使将来新增保留字段检查遗漏，也不会被用户 SET 覆盖。
 *
 * @param {Object} userSettings
 * @param {Object} auditSettings
 * @return {Object}
 */
function mergeSqlTaskSettings_(userSettings, auditSettings) {
  var settings = {};
  userSettings = userSettings || {};
  auditSettings = auditSettings || {};

  Object.keys(userSettings).forEach(function(key) {
    settings[key] = userSettings[key];
  });
  Object.keys(auditSettings).forEach(function(key) {
    settings[key] = auditSettings[key];
  });

  return settings;
}

/**
 * XML 转义
 *
 * @param {string} str - 原始字符串
 * @return {string} 转义后的字符串
 */
function escapeXml_(str) {
  if (!str) return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}


// ============================================================
// XML 解析
// ============================================================

/**
 * 解析作业状态响应（XML）- 只解析 Instance 状态
 *
 * @param {string} xml - XML 响应字符串
 * @return {Object} { instanceId: string, status: string }
 */
function parseInstanceStatusXml_(xml) {
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();

  var instanceId = getChildText_(root, 'Name');
  var status = getChildText_(root, 'Status');

  return {
    instanceId: instanceId,
    status: status  // Running | Suspended | Terminated
  };
}

/**
 * 解析 Task 状态响应（XML）
 *
 * @param {string} xml - XML 响应字符串
 * @return {Object} { taskName: string, taskStatus: string }
 */
function parseTaskStatusXml_(xml) {
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();

  var taskName = null;
  var taskStatus = null;

  var tasks = root.getChild('Tasks');
  if (tasks) {
    var task = tasks.getChild('Task');
    if (task) {
      taskName = getChildText_(task, 'Name');
      taskStatus = getChildText_(task, 'Status');
    }
  }

  return {
    taskName: taskName || SQL_TASK_NAME,
    taskStatus: taskStatus  // Waiting | Running | Success | Failed | Cancelled
  };
}

/**
 * 解析作业结果响应（XML）
 *
 * @param {string} xml - XML 响应字符串
 * @return {Object} { columns: string[], rows: string[][], rowCount: number, rawResult: string }
 */
function parseInstanceResultXml_(xml) {
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();

  var tasks = root.getChild('Tasks');
  if (!tasks) {
    throw new Error('响应中缺少 Tasks 元素');
  }

  var task = tasks.getChild('Task');
  if (!task) {
    throw new Error('响应中缺少 Task 元素');
  }

  var status = getChildText_(task, 'Status');
  if (status && status.toUpperCase() !== 'SUCCESS') {
    // 获取失败结果
    var resultElem = task.getChild('Result');
    var rawResult = '';
    if (resultElem) {
      var transform = resultElem.getAttribute('Transform');
      var transformValue = transform ? transform.getValue() : '';
      var resultText = resultElem.getText();
      if (transformValue === 'Base64' && resultText) {
        var bytes = Utilities.base64Decode(resultText);
        rawResult = Utilities.newBlob(bytes).getDataAsString();
      } else {
        rawResult = resultText;
      }
    }
    return { columns: [], rows: [], rowCount: 0, rawResult: rawResult, taskStatus: status };
  }

  // 获取 Result 元素
  var resultElem = task.getChild('Result');
  if (!resultElem) {
    // 空结果
    return { columns: [], rows: [], rowCount: 0, rawResult: '' };
  }

  var transform = resultElem.getAttribute('Transform');
  var transformValue = transform ? transform.getValue() : '';
  var resultText = resultElem.getText();

  // Base64 解码
  var decodedResult;
  if (transformValue === 'Base64') {
    var bytes = Utilities.base64Decode(resultText);
    decodedResult = Utilities.newBlob(bytes).getDataAsString();
  } else {
    decodedResult = resultText;
  }

  // 解析 ResultDescriptor 获取列信息
  var descriptorElem = task.getChild('ResultDescriptor');
  var columns = [];

  if (descriptorElem) {
    var descriptorText = descriptorElem.getText();
    try {
      var descriptor = JSON.parse(descriptorText);
      if (descriptor.Schema && descriptor.Schema.Columns) {
        columns = descriptor.Schema.Columns.map(function(col) {
          return col.Name || col.name || '';
        });
      }
    } catch (e) {
      Logger.log('[SqlExecutor] ResultDescriptor 解析失败，将使用 CSV 第一行作为列名');
    }
  }

  // 解析 CSV
  var csvRows = [];
  if (decodedResult && decodedResult.trim()) {
    csvRows = Utilities.parseCsv(decodedResult);
  }

  // 如果没有从 ResultDescriptor 获取列名，使用 CSV 第一行
  if (columns.length === 0 && csvRows.length > 0) {
    columns = csvRows[0];
    csvRows = csvRows.slice(1);
  } else if (csvRows.length > 0) {
    // CSV 第一行是列名，跳过
    csvRows = csvRows.slice(1);
  }

  // 将 \N (NULL) 替换为空字符串
  csvRows = csvRows.map(function(row) {
    return row.map(function(cell) {
      return cell === '\N' ? '' : cell;
    });
  });

  return {
    columns: columns,
    rows: csvRows,
    rowCount: csvRows.length,
    rawResult: decodedResult
  };
}

function parseInstanceResultXmlSafe_(xml) {
  try {
    return parseInstanceResultXml_(xml);
  } catch (e) {
    throw new Error('解析结果失败: ' +
      String(e && e.message ? e.message : e || ''));
  }
}

/**
 * 解析错误响应（XML）
 *
 * @param {string} xml - XML 错误响应
 * @return {string} 错误信息
 */
function parseErrorXml_(xml) {
  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();

    // 尝试解析 Error 元素
    if (root.getName() === 'Error') {
      var code = getChildText_(root, 'Code');
      var message = getChildText_(root, 'Message');
      return (code ? code + ': ' : '') + (message || xml.substring(0, 200));
    }

    return xml.substring(0, 200);
  } catch (e) {
    return xml.substring(0, 200);
  }
}

/**
 * 解析 MaxCompute 错误响应，返回 Code + Message 拼接后的原文，
 * 与 MaxCompute 服务端审计可见的信息一致。
 *
 * @param {string} xml
 * @return {string}
 */
function parseErrorSummary_(xml) {
  xml = String(xml || '');
  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();
    if (root.getName() === 'Error') {
      var code = getChildText_(root, 'Code') || 'UnknownError';
      var message = getChildText_(root, 'Message') || '';
      return code + ': ' + message;
    }
  } catch (e) {}

  return xml;
}

/**
 * 安全获取子元素文本
 *
 * @param {Element} parent - 父元素
 * @param {string} childName - 子元素名称
 * @return {string}
 */
function getChildText_(parent, childName) {
  if (!parent) return '';
  var child = parent.getChild(childName);
  return child ? child.getText() : '';
}
