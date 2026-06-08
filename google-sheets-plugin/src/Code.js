// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * MaxCompute Query Add-on for Google Sheets
 *
 * 功能：
 * 1. 在菜单栏添加「MaxCompute」菜单
 * 2. 打开侧边栏，用户输入 SQL
 * 3. 直接调用 MaxCompute Instance API 执行查询
 * 4. 将结果写入当前 Sheet
 */

/** 查询结果最多写入 Sheet 的行数 */
var MAX_RESULT_ROWS = 10000;

/** 等待写入 Sheet 锁的最长时间（毫秒） */
var SHEET_WRITE_LOCK_TIMEOUT_MS = 30000;

/** 无表格结果时写入 Sheet 的提示 */
var EMPTY_RESULT_MESSAGE = 'Query completed successfully. No tabular result returned.';

/** 默认结果 Sheet 名 */
var DEFAULT_RESULT_SHEET_NAME = 'Query Result';

/** Google Sheets 工作表名称最长 100 字符 */
var MAX_SHEET_NAME_LENGTH = 100;

/**
 * 插件安装时的回调
 */
function onInstall(e) {
  onOpen(e);
}


// ============================================================
// 菜单与侧边栏
// ============================================================

/**
 * Sheets 打开时自动创建菜单
 */
function onOpen() {
  var lang = getUserLanguage();
  var isZh = (lang === 'zh');
  var ui = SpreadsheetApp.getUi();

  // 先创建子菜单（独立的 Menu 对象）
  var langMenu = ui.createMenu(isZh ? '语言' : 'Language')
    .addItem('中文', 'switchLanguageToZh')
    .addItem('English', 'switchLanguageToEn');

  // 再创建主菜单，把子菜单挂进去
  ui.createAddonMenu()
    .addItem(isZh ? '打开查询面板' : 'Open Query Panel', 'showSidebar')
    .addItem(isZh ? '设置连接' : 'Settings', 'showSettings')
    .addSeparator()
    .addSubMenu(langMenu)
    .addSeparator()
    .addItem(isZh ? '清空当前 Sheet' : 'Clear Current Sheet', 'clearCurrentSheet')
    .addToUi();
}


/**
 * 打开查询侧边栏
 *
 * 性能优化：使用 HtmlTemplate 把 language / connection / sheetNames 直接注入页面，
 * 避免 sidebar 首屏发起 3 次 google.script.run 往返（每次 ~200-800ms）。
 */
function showSidebar() {
  var config = getMcConfig_();
  try {
    assertUsableMcConfig_(config);
  } catch (e) {
    // 未配置，跳转到设置页面
    showSettings();
    return;
  }

  // 收集首屏需要的全部初始数据
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  var sheetNames;
  try {
    sheetNames = ss.getSheets().map(function(s) {
      return s.getName();
    });
  } catch (e) {
    sheetNames = [];
  }

  var activeSheet = { id: 0, name: '' };
  var activeSheetSql = null;
  try {
    var as = ss.getActiveSheet();
    activeSheet = { id: as.getSheetId(), name: as.getName() };
    activeSheetSql = loadSheetSql(activeSheet.id);
  } catch (e) {}

  // 预加载 Job 列表和 Schedule 列表
  var jobList = [];
  try { jobList = readJobList_(); } catch (e) {}

  var scheduleList = [];
  try { scheduleList = readScheduleList_(); } catch (e) {}

  var ossConfigured = false;
  try { ossConfigured = getOssExportStatus().configured; } catch (e) {}

  var exportPreferences = { prefix: '', template: '{prefix}{sheet}_{date}.csv' };
  try { exportPreferences = getExportPreferences(); } catch (e) {}

  var initialData = {
    language: getUserLanguage(),
    connection: {
      configured: true
    },
    sheetNames: sheetNames,
    activeSheet: activeSheet,
    activeSheetSql: activeSheetSql,
    jobList: jobList,
    scheduleList: scheduleList,
    ossConfigured: ossConfigured,
    exportPreferences: exportPreferences
  };

  var tpl = HtmlService.createTemplateFromFile('Sidebar');
  tpl.initialData = toSafeScriptJson_(initialData);

  var html = tpl.evaluate()
    .setTitle('MaxCompute')
    .setWidth(360);
  SpreadsheetApp.getUi().showSidebar(html);
}

/**
 * 清空当前 Sheet
 */
function clearCurrentSheet() {
  var lang = getUserLanguage();
  var isZh = (lang === 'zh');
  var ui = SpreadsheetApp.getUi();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getActiveSheet();
  var title = isZh ? '确认清空' : 'Confirm Clear';
  var message = isZh
    ? '确定要清空当前 Sheet「' + sheet.getName() + '」的全部内容和格式吗？此操作无法撤销。'
    : 'Clear all content and formatting in the current sheet "' + sheet.getName() + '"? This cannot be undone.';

  var choice = ui.alert(title, message, ui.ButtonSet.OK_CANCEL);
  if (choice !== ui.Button.OK) {
    ss.toast(isZh ? '已取消清空' : 'Clear cancelled', 'MaxCompute');
    return;
  }

  sheet.clear();
  ss.toast(isZh ? '已清空当前 Sheet' : 'Current sheet cleared', 'MaxCompute');
}


// ============================================================
// 语言切换
// ============================================================

/**
 * 切换到英文
 */
function switchLanguageToEn() {
  setLanguage_('en');
  SpreadsheetApp.getActiveSpreadsheet().toast('Switched to English', 'MaxCompute');
  onOpen();
}

/**
 * 切换到中文
 */
function switchLanguageToZh() {
  setLanguage_('zh');
  SpreadsheetApp.getActiveSpreadsheet().toast('已切换到中文', 'MaxCompute');
  onOpen();
}

/**
 * 设置语言（内部函数）
 */
function setLanguage_(lang) {
  var props = PropertiesService.getUserProperties();
  props.setProperty('MC_LANGUAGE', lang);
}


// ============================================================
// 核心：执行 SQL 查询
// ============================================================

/**
 * 由侧边栏调用，执行 SQL 并将结果写入 Sheet
 *
 * @param {string} sql - 用户输入的 SQL
 * @param {number} maxRows - 最大写入行数，最多 10000 行
 * @param {string} targetSheet - 目标 Sheet 名（默认 "Query Result"）
 * @param {number} [timeoutSeconds] - 客户端轮询超时（秒），由 Sidebar 当前会话指定
 * @return {Object} 执行结果摘要，返回给侧边栏显示
 */
function executeQuery(sql, maxRows, targetSheet, timeoutSeconds) {
  // ---- 参数校验 ----
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }
  targetSheet = normalizeSheetName_(targetSheet);

  // ---- 检查配置 ----
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  // ---- 日志：触发用户、目标表格 ----
  var ctx = getQueryContext_(targetSheet);
  Logger.log('[Code.executeQuery] user=' + ctx.user +
    ' spreadsheetName=' + ctx.spreadsheetName +
    ' spreadsheetId=' + ctx.spreadsheetId +
    ' targetSheet=' + ctx.targetSheet +
    ' project=' + config.project +
    ' timeoutSec=' + (timeoutSeconds || 'default') +
    ' sql=' + sql);

  // ---- 执行 SQL ----
  var result = executeSqlQuery_(sql.trim(), timeoutSeconds, ctx);

  // ---- 写入 Sheet ----
  var data = prepareResultData_(result, maxRows);
  writePreparedResultToSheet_(data, targetSheet);

  // ---- 返回摘要给侧边栏 ----
  return buildQuerySummary_(data, result.instanceId || '', targetSheet);
}

/**
 * 提交 SQL 查询（只提交，不等待）
 * 用于前端显示 instance id
 *
 * @param {string} sql - SQL 语句
 * @param {number} maxRows - 最大写入行数，最多 10000 行
 * @param {string} targetSheet - 目标 Sheet 名
 * @return {Object} { instanceId: string, logviewUrl: string } 或 { sync: true, ... } (同步执行)
 */
function submitQuery(sql, maxRows, targetSheet) {
  // ---- 参数校验 ----
  if (!sql || !sql.trim()) {
    throw new Error('SQL 不能为空');
  }

  // ---- 检查配置 ----
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  // ---- 日志 ----
  targetSheet = normalizeSheetName_(targetSheet);

  var ctx = getQueryContext_(targetSheet);
  Logger.log('[Code.submitQuery] user=' + ctx.user +
    ' spreadsheetName=' + ctx.spreadsheetName +
    ' spreadsheetId=' + ctx.spreadsheetId +
    ' targetSheet=' + ctx.targetSheet +
    ' project=' + config.project +
    ' sql=' + sql);

  // ---- 提交作业 ----
  var result = submitSqlJobOnly_(sql.trim(), ctx);

  if (result.sync) {
    var data = prepareResultData_(result.result, maxRows);
    writePreparedResultToSheet_(data, targetSheet);
    var summary = buildQuerySummary_(data, '', targetSheet);
    summary.sync = true;
    return summary;
  }

  // 添加 logviewUrl
  if (result.instanceId) {
    result.logviewUrl = buildLogviewUrl_(result.instanceId);
  }

  return result;
}

/**
 * 查询作业进度（前端轮询调用，单次返回，不阻塞）
 *
 * @param {string} instanceId - 作业 ID
 * @return {Object} {
 *   instanceTerminated: boolean,
 *   instanceStatus: string,        // Running | Suspended | Terminated
 *   taskStatus?: string,           // Waiting | Running | Success | Failed | Cancelled
 *   errorSummary?: string          // taskStatus === 'Failed' 时附带安全摘要
 * }
 */
function getQueryProgress(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);
  return getJobProgress_(instanceId);
}

/**
 * 拉取已完成作业的结果并写入 Sheet
 * （前端确认 task 已 Success 后调用）
 *
 * @param {string} instanceId - 作业 ID
 * @param {string} targetSheet - 目标 Sheet 名
 * @param {number} maxRows - 最大写入行数，最多 10000 行
 * @return {Object} 执行结果摘要
 */
function writeQueryResult(instanceId, targetSheet, maxRows) {
  instanceId = normalizeInstanceId_(instanceId);
  targetSheet = normalizeSheetName_(targetSheet);

  // ---- 日志 ----
  var ctx = getQueryContext_(targetSheet);
  var cfg = getMcConfig_();
  Logger.log('[Code.writeQueryResult] user=' + ctx.user +
    ' spreadsheetName=' + ctx.spreadsheetName +
    ' spreadsheetId=' + ctx.spreadsheetId +
    ' targetSheet=' + ctx.targetSheet +
    ' project=' + cfg.project +
    ' instanceId=' + instanceId);

  // ---- 拉取结果 ----
  var result = getJobResult_(instanceId);

  var data = prepareResultData_(result, maxRows);
  writePreparedResultToSheet_(data, targetSheet);

  return buildQuerySummary_(data, instanceId, targetSheet);
}

/**
 * 取消正在运行的查询（供 Sidebar Cancel 按钮调用）
 *
 * @param {string} instanceId - 作业 ID
 * @return {Object} { instanceId, killResult }
 *         killResult: 'ok' | 'already_terminated' | 'failed:<msg>'
 */
function cancelQuery(instanceId) {
  instanceId = normalizeInstanceId_(instanceId);

  var ctx = getQueryContext_(null);
  var cfg = getMcConfig_();
  Logger.log('[Code.cancelQuery] user=' + ctx.user +
    ' spreadsheetName=' + ctx.spreadsheetName +
    ' spreadsheetId=' + ctx.spreadsheetId +
    ' project=' + cfg.project +
    ' instanceId=' + instanceId);

  var killResult = cancelSqlJob_(instanceId);
  return {
    instanceId: instanceId,
    killResult: killResult
  };
}


// ============================================================
// 写入 Sheet
// ============================================================

/**
 * 规范化查询结果，并强制执行最多 10000 行的写入限制。
 *
 * @param {Object} result - { columns, rows, rowCount }
 * @param {number} maxRows - 用户指定最大写入行数
 * @return {Object}
 */
function prepareResultData_(result, maxRows) {
  result = result || {};

  var columns = normalizeResultColumns_(result.columns || []);
  var rows = normalizeResultRows_(result.rows || [], columns.length);
  var totalRowCount = typeof result.rowCount === 'number' ? result.rowCount : rows.length;
  var rowLimit = resolveResultRowLimit_(maxRows);
  var truncated = rows.length > rowLimit;

  if (truncated) {
    rows = rows.slice(0, rowLimit);
  }

  return {
    columns: columns,
    rows: rows,
    row_count: rows.length,
    total_row_count: totalRowCount,
    truncated: truncated,
    emptyResult: columns.length === 0
  };
}

/**
 * 解析最大写入行数，硬限制不超过 MAX_RESULT_ROWS。
 */
function resolveResultRowLimit_(maxRows) {
  var n = parseInt(maxRows, 10);
  if (isNaN(n) || n <= 0) {
    return MAX_RESULT_ROWS;
  }
  return Math.min(n, MAX_RESULT_ROWS);
}

/**
 * 规整列名，避免空列名或非字符串值导致结果难读。
 */
function normalizeResultColumns_(columns) {
  columns = columns || [];
  return columns.map(function(col, index) {
    col = col === null || col === undefined ? '' : String(col);
    return sanitizeSpreadsheetCell_(col || ('Column ' + (index + 1)));
  });
}

/**
 * 规整结果行，使每行列数都与表头一致，满足 Range.setValues() 的矩阵要求。
 */
function normalizeResultRows_(rows, columnCount) {
  rows = rows || [];
  return rows.map(function(row) {
    row = row || [];
    var normalized = [];
    for (var i = 0; i < columnCount; i++) {
      var cell = i < row.length ? row[i] : '';
      normalized.push(sanitizeSpreadsheetCell_(cell === null || cell === undefined ? '' : cell));
    }
    return normalized;
  });
}

/**
 * 避免 MaxCompute 返回值被 Google Sheets 当作公式执行。
 *
 * Range.setValues() 会把以公式触发字符开头的字符串解释为公式；查询结果应作为数据
 * 写入，因此在进入 Sheet 前统一转义为文本。
 */
function sanitizeSpreadsheetCell_(cell) {
  if (typeof cell !== 'string') {
    return cell;
  }
  if (/^[\s]*[=+\-@]/.test(cell) || /^[\t\n]/.test(cell)) {
    return "'" + cell;
  }
  return cell;
}

/**
 * 规范化目标工作表名称，避免非法字符、空名称或超长名称导致 insertSheet 失败。
 */
function normalizeSheetName_(sheetName) {
  sheetName = String(sheetName || '').trim();
  if (!sheetName) {
    sheetName = DEFAULT_RESULT_SHEET_NAME;
  }

  sheetName = sheetName
    .replace(/[\[\]\*\?\/\:]/g, '_')
    .replace(/[\n\t]+/g, ' ')
    .replace(/^\s+|\s+$/g, '');

  if (!sheetName) {
    sheetName = DEFAULT_RESULT_SHEET_NAME;
  }

  if (sheetName.length > MAX_SHEET_NAME_LENGTH) {
    sheetName = sheetName.substring(0, MAX_SHEET_NAME_LENGTH);
  }

  return sheetName;
}

/**
 * 写入规范化后的查询结果。
 */
function writePreparedResultToSheet_(data, sheetName, spreadsheetId) {
  withDocumentLock_(function() {
    if (!data.columns || data.columns.length === 0) {
      writeEmptyResultToSheet_(sheetName, spreadsheetId);
      return;
    }
    writeResultToSheet_(data, sheetName, spreadsheetId);
  });
}

/**
 * 无表格结果时仍写入一个明确状态页，避免用户误以为插件失败。
 */
function writeEmptyResultToSheet_(sheetName, spreadsheetId) {
  writeResultToSheet_({
    columns: ['Status'],
    rows: [[EMPTY_RESULT_MESSAGE]],
    row_count: 1
  }, sheetName, spreadsheetId);
}

/**
 * 构建返回给侧边栏的执行摘要。
 */
function buildQuerySummary_(data, instanceId, targetSheet) {
  return {
    rowCount: data.row_count,
    totalRowCount: data.total_row_count,
    columnCount: data.columns ? data.columns.length : 0,
    truncated: !!data.truncated,
    emptyResult: !!data.emptyResult,
    instanceId: instanceId || '',
    logviewUrl: buildLogviewUrl_(instanceId),
    sheetName: targetSheet
  };
}

/**
 * 使用锁串行化 Sheet 写入，避免并发查询互相 clear/write。
 *
 * 优先使用 DocumentLock（同一文档并发保护），
 * 如果在 time-driven trigger 上下文中 DocumentLock 不可用则降级为 ScriptLock。
 *
 * @param {Function} fn
 * @return {*}
 */
function withDocumentLock_(fn) {
  var lock = null;
  try {
    lock = LockService.getDocumentLock();
  } catch (e) {
    // time-driven trigger 上下文无 document，降级为 ScriptLock
  }
  if (!lock) {
    lock = LockService.getScriptLock();
  }
  if (!lock.tryLock(SHEET_WRITE_LOCK_TIMEOUT_MS)) {
    throw new Error('当前表格正在写入查询结果，请稍后重试。');
  }

  try {
    return fn();
  } finally {
    lock.releaseLock();
  }
}

/**
 * 将查询结果写入指定 Sheet
 *
 * @param {Object} data - { columns: string[], rows: string[][], row_count: number }
 * @param {string} sheetName - 目标 Sheet 名
 */
function writeResultToSheet_(data, sheetName, spreadsheetId) {
  sheetName = normalizeSheetName_(sheetName);
  var ss = null;
  if (spreadsheetId) {
    try { ss = SpreadsheetApp.openById(spreadsheetId); } catch (e) {}
  }
  if (!ss) {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }
  if (!ss) {
    throw new Error('无法访问目标 Spreadsheet（定时触发器需要 spreadsheetId）');
  }

  // 获取或创建目标 Sheet
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  } else {
    sheet.clear();
    // 清除所有格式
    sheet.getRange(1, 1, sheet.getMaxRows(), sheet.getMaxColumns())
      .clearFormat();
  }

  var columns = data.columns;
  var rows = data.rows;
  var numCols = columns.length;

  // ---- 写入表头 ----
  var headerRange = sheet.getRange(1, 1, 1, numCols);
  headerRange.setValues([columns]);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#4285F4');
  headerRange.setFontColor('#FFFFFF');
  headerRange.setHorizontalAlignment('center');

  // ---- 写入数据 ----
  if (rows.length > 0) {
    // 分批写入，避免超出 Apps Script 执行时间限制
    // 每批最多 10000 行
    var BATCH_SIZE = 10000;
    for (var i = 0; i < rows.length; i += BATCH_SIZE) {
      var batch = rows.slice(i, i + BATCH_SIZE);
      var startRow = i + 2;  // 第一行是表头
      sheet.getRange(startRow, 1, batch.length, numCols).setValues(batch);
    }

    // 隔行变色（数据量不大时）
    if (rows.length <= 1000) {
      for (var r = 0; r < rows.length; r++) {
        if (r % 2 === 1) {
          sheet.getRange(r + 2, 1, 1, numCols).setBackground('#F8F9FA');
        }
      }
    }
  }

  // ---- 冻结表头 ----
  sheet.setFrozenRows(1);

  // ---- 自适应列宽（列数不多时）----
  if (numCols <= 20) {
    for (var c = 1; c <= numCols; c++) {
      sheet.autoResizeColumn(c);
    }
  }

  // 激活该 Sheet（仅在有 UI 上下文时有效）
  try { sheet.activate(); } catch (e) {}

  Logger.log('[Code.writeResultToSheet] sheetName=' + sheetName +
    ' rows=' + rows.length + ' cols=' + numCols);
}


// ============================================================
// 工具函数：日志上下文
// ============================================================

/**
 * 获取当前查询的执行上下文（提交人邮箱 / 表格信息），用于作业审计和安全日志摘要
 *
 * @param {string} targetSheet - 目标 Sheet 名（可选）
 * @return {Object} { user, spreadsheetName, spreadsheetId, targetSheet }
 */
function getQueryContext_(targetSheet) {
  var user = getCurrentUserAuditKey_();

  var ssName = '';
  var ssId = '';
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) {
      ssName = ss.getName();
      ssId = ss.getId();
    }
  } catch (e) {}

  return {
    user: user,
    spreadsheetName: ssName,
    spreadsheetId: ssId,
    targetSheet: targetSheet || ''
  };
}

/**
 * 将对象序列化为可安全嵌入 <script> 的 JSON 字面量。
 *
 * HtmlTemplate 的 force-print 不做上下文转义，因此必须避免 `</script>`
 * 等字符序列破坏脚本上下文。
 */
function toSafeScriptJson_(value) {
  return JSON.stringify(value)
    .replace(/</g, '\u003c')
    .replace(/>/g, '\u003e')
    .replace(/&/g, '\u0026')
    .replace(new RegExp('\u2028', 'g'), '\u2028')
    .replace(new RegExp('\u2029', 'g'), '\u2029');
}


// ============================================================
// 工具函数：供侧边栏调用
// ============================================================

/**
 * 获取当前所有 Sheet 名称列表（供侧边栏下拉选择）
 * @return {string[]}
 */
function getSheetNames() {
  return SpreadsheetApp.getActiveSpreadsheet()
    .getSheets()
    .map(function(s) { return s.getName(); });
}

function activateSheet(sheetName) {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (sheet) sheet.activate();
}

function saveSheetSql(sheetId, data) {
  var props = PropertiesService.getDocumentProperties();
  props.setProperty('mc_sheet_sql_' + sheetId, JSON.stringify(data));
}

function loadSheetSql(sheetId) {
  var props = PropertiesService.getDocumentProperties();
  var raw = props.getProperty('mc_sheet_sql_' + sheetId);
  if (!raw) return null;
  try { return JSON.parse(raw); } catch (e) { return null; }
}

function getActiveSheetInfo() {
  var sheet = SpreadsheetApp.getActiveSpreadsheet().getActiveSheet();
  return { id: sheet.getSheetId(), name: sheet.getName() };
}

function switchSheet(currentSheetId, currentData, targetSheetName) {
  if (currentSheetId && currentData) {
    saveSheetSql(currentSheetId, currentData);
  }
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var targetSheet = ss.getSheetByName(targetSheetName);
  if (!targetSheet) {
    throw new Error('Sheet not found: ' + targetSheetName);
  }
  targetSheet.activate();
  var targetId = targetSheet.getSheetId();
  var savedSql = loadSheetSql(targetId);
  return { id: targetId, name: targetSheetName, sqlData: savedSql };
}

function getAllSheetSqlBindings() {
  var props = PropertiesService.getDocumentProperties();
  var all = props.getProperties();
  var bindings = [];
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheets = ss.getSheets();
  var sheetMap = {};
  sheets.forEach(function(s) { sheetMap[s.getSheetId()] = s.getName(); });

  Object.keys(all).forEach(function(key) {
    if (key.indexOf('mc_sheet_sql_') !== 0) return;
    var sheetId = key.replace('mc_sheet_sql_', '');
    var sheetName = sheetMap[sheetId];
    if (!sheetName) return;
    try {
      var data = JSON.parse(all[key]);
      if (data && data.mode === 'sql' && data.sql) {
        bindings.push({ sheetId: sheetId, targetSheet: sheetName, sql: data.sql });
      }
    } catch (e) {}
  });
  return bindings;
}

/**
 * 获取连接状态（供侧边栏显示）
 * @return {Object}
 */
function getConnectionStatus() {
  try {
    var config = getMcConfig_();
    assertUsableMcConfig_(config);
    return {
      configured: true
    };
  } catch (e) {
    return {
      configured: false,
      error: getConnectionErrorMessage_(e)
    };
  }
}

/**
 * 快速健康检查（测试 MaxCompute API 是否可达）
 * @return {Object}
 */
function testConnection() {
  var config = getMcConfig_();
  var lang = getUserLanguage();
  var isZh = lang === 'zh';

  try {
    assertUsableMcConfig_(config);
  } catch (e) {
    return { success: false, message: getConnectionErrorMessage_(e, isZh) };
  }

  try {
    // 尝试列出 Schema 作为健康检查
    var schemas = listSchemas_();
    return {
      success: true,
      message: isZh ? '连接正常 (Schema: ' + schemas.length + ')' : 'Connection OK (Schemas: ' + schemas.length + ')'
    };
  } catch (e) {
    return { success: false, message: (isZh ? '连接失败: ' : 'Connection failed: ') + getConnectionErrorMessage_(e, isZh) };
  }
}

/**
 * 把后端连接错误规范化成对用户友好的提示。配置类错误返回可操作指引，
 * 其余错误直接透传 MaxCompute 服务端返回的原文，便于排查。
 */
function getConnectionErrorMessage_(e, isZh) {
  var message = String(e && e.message ? e.message : e || '');
  if (/Endpoint 格式不正确/.test(message)) {
    return 'Endpoint 格式不正确，应为 https://service.{region}.maxcompute.aliyun.com/api';
  }
  if (/请先配置 AccessKey、Project 和 Endpoint/.test(message)) {
    return isZh === false ?
      'Please configure AccessKey, Project, and Endpoint first' :
      '请先配置 AccessKey、Project 和 Endpoint（MaxCompute → 设置连接）';
  }
  return message;
}

// ============================================================
// 数据目录 API（供侧边栏调用）
// ============================================================

/**
 * 获取 Schema 列表
 * @return {Object[]} Schema 列表
 */
function getSchemas() {
  return listSchemas_();
}

/**
 * 获取指定 Schema 下的表列表
 * @param {string} schemaName - Schema 名称
 * @param {string} prefix - 表名前缀过滤（可选）
 * @return {Object[]} 表列表
 */
function getTables(schemaName, prefix) {
  return listTables_(schemaName, prefix);
}

/**
 * 获取表详情（包含字段，不含分区列表）
 * @param {string} tableName - 表名
 * @param {string} schemaName - Schema 名称（可选）
 * @return {Object} 表详情
 */
function getTableDetail(tableName, schemaName) {
  var schema = getTableSchema_(tableName, schemaName);
  return {
    name: schema.name,
    type: schema.type,
    comment: schema.comment,
    columns: schema.columns,
    partitionColumns: schema.partitionColumns,
    partitionCount: 0, // 将由前端 lazy 加载
    schemaName: schemaName || 'default'
  };
}

/**
 * 获取分区列表（lazy 加载）
 * @param {string} tableName - 表名
 * @param {string} schemaName - Schema 名称（可选）
 * @return {Object[]} 分区列表
 */
function getPartitions(tableName, schemaName) {
  return listPartitions_(tableName, schemaName);
}

/**
 * 获取用户语言偏好
 * @return {string}
 */
function getUserLanguage() {
  try {
    var props = PropertiesService.getUserProperties();
    return props.getProperty('MC_LANGUAGE') || 'en';
  } catch(e) {
    return 'en';
  }
}


// ============================================================
// 查询历史（PropertiesService 跨设备同步）
//
// 仅做"最近 10 条 SQL + 最近 10 个 Instance ID"的轻量级同步，配合
// localStorage 写穿透：前端立刻渲染，同时 fire-and-forget 把变更同步到
// 用户的 PropertiesService。这只是"我自己常用的几条"，不是组织级查询档案。
// ============================================================

var MC_SQL_HISTORY_KEY = 'MC_SQL_HISTORY';
var MC_SQL_HISTORY_ENABLED_KEY = 'MC_SQL_HISTORY_ENABLED';
var MC_INSTANCE_HISTORY_KEY = 'MC_INSTANCE_HISTORY';
var MAX_SQL_HISTORY_ENTRIES = 10;
var MAX_INSTANCE_HISTORY_ENTRIES = 10;
// 单条 SQL 写入 PropertiesService 时的最大长度（按字符数估算 UTF-8 上限）。
// 10 × 4 KB ≈ 40 KB，远小于 Apps Script 每条属性 ~9 KB / 每用户 500 KB 的限制。
var MAX_SQL_HISTORY_ENTRY_CHARS = 4 * 1024;
var INSTANCE_HISTORY_TTL_MS = 24 * 60 * 60 * 1000;

function getQueryHistory() {
  return {
    sqlItems: readSqlHistory_(),
    instanceItems: readInstanceHistory_(Date.now()),
    enabled: isSqlHistoryEnabled_()
  };
}

function appendSqlHistory(sql) {
  if (!isSqlHistoryEnabled_()) {
    return { items: [] };
  }
  sql = String(sql == null ? '' : sql);
  if (sql.length > MAX_SQL_HISTORY_ENTRY_CHARS) {
    sql = sql.slice(0, MAX_SQL_HISTORY_ENTRY_CHARS);
  }
  if (!sql.trim()) {
    return { items: readSqlHistory_() };
  }
  var items = readSqlHistory_().filter(function(item) { return item !== sql; });
  items.unshift(sql);
  if (items.length > MAX_SQL_HISTORY_ENTRIES) {
    items = items.slice(0, MAX_SQL_HISTORY_ENTRIES);
  }
  writeSqlHistory_(items);
  return { items: items };
}

function removeSqlHistoryAt(index) {
  var items = readSqlHistory_();
  var i = parseInt(index, 10);
  if (!isNaN(i) && i >= 0 && i < items.length) {
    items.splice(i, 1);
    writeSqlHistory_(items);
  }
  return { items: items };
}

function clearSqlHistory() {
  writeSqlHistory_([]);
  return { items: [] };
}

function setSqlHistoryEnabled(enabled) {
  var bool = !!enabled;
  var props = PropertiesService.getUserProperties();
  props.setProperty(MC_SQL_HISTORY_ENABLED_KEY, bool ? 'true' : 'false');
  if (!bool) {
    writeSqlHistory_([]);
  }
  return { enabled: bool };
}

function appendInstanceHistory(instanceId) {
  instanceId = String(instanceId || '').trim();
  if (!isValidInstanceIdForHistory_(instanceId)) {
    return { items: readInstanceHistory_(Date.now()) };
  }
  var now = Date.now();
  var items = readInstanceHistory_(now).filter(function(item) {
    return item.instanceId !== instanceId;
  });
  items.unshift({ instanceId: instanceId, savedAt: now });
  if (items.length > MAX_INSTANCE_HISTORY_ENTRIES) {
    items = items.slice(0, MAX_INSTANCE_HISTORY_ENTRIES);
  }
  writeInstanceHistory_(items);
  return { items: items };
}

function readSqlHistory_() {
  if (!isSqlHistoryEnabled_()) {
    return [];
  }
  var raw = PropertiesService.getUserProperties().getProperty(MC_SQL_HISTORY_KEY);
  if (!raw) {
    return [];
  }
  try {
    var parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(function(item) { return typeof item === 'string' && item; })
      .slice(0, MAX_SQL_HISTORY_ENTRIES);
  } catch (e) {
    return [];
  }
}

function writeSqlHistory_(items) {
  var props = PropertiesService.getUserProperties();
  if (!items || items.length === 0) {
    props.deleteProperty(MC_SQL_HISTORY_KEY);
    return;
  }
  props.setProperty(MC_SQL_HISTORY_KEY, JSON.stringify(items));
}

function readInstanceHistory_(now) {
  var raw = PropertiesService.getUserProperties().getProperty(MC_INSTANCE_HISTORY_KEY);
  if (!raw) {
    return [];
  }
  try {
    var parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map(function(item) {
        if (!item) return null;
        var id = String(item.instanceId || '').trim();
        var savedAt = parseInt(item.savedAt, 10) || 0;
        if (!isValidInstanceIdForHistory_(id)) return null;
        return { instanceId: id, savedAt: savedAt };
      })
      .filter(function(item) {
        return item && (now - item.savedAt) <= INSTANCE_HISTORY_TTL_MS;
      })
      .slice(0, MAX_INSTANCE_HISTORY_ENTRIES);
  } catch (e) {
    return [];
  }
}

function writeInstanceHistory_(items) {
  var props = PropertiesService.getUserProperties();
  if (!items || items.length === 0) {
    props.deleteProperty(MC_INSTANCE_HISTORY_KEY);
    return;
  }
  props.setProperty(MC_INSTANCE_HISTORY_KEY, JSON.stringify(items));
}

function isSqlHistoryEnabled_() {
  var v = PropertiesService.getUserProperties().getProperty(MC_SQL_HISTORY_ENABLED_KEY);
  return v !== 'false';
}

function isValidInstanceIdForHistory_(instanceId) {
  return !!instanceId &&
    instanceId.length <= 128 &&
    /^[A-Za-z0-9_.:-]+$/.test(instanceId) &&
    instanceId.indexOf('..') === -1;
}

/**
 * 生成 Logview URL
 *
 * @param {string} instanceId - 实例 ID
 * @return {string} Logview URL
 */
function buildLogviewUrl_(instanceId) {
  var config = getMcConfig_();
  if (!config.endpoint || !config.project || !instanceId) {
    return '';
  }

  // 从 endpoint 提取 regionId
  // 例如: https://service.ap-northeast-2.maxcompute.aliyun.com/api -> ap-northeast-2
  var endpoint = config.endpoint;
  var regionMatch = endpoint.match(/service\.([^.]+)\.maxcompute\.aliyun\.com/);
  var regionId = regionMatch ? regionMatch[1] : 'cn-hangzhou';

  // endpoint 直接使用，包含 /api 后缀
  return 'https://maxcompute.console.aliyun.com/' + regionId +
    '/job-insights?h=' + encodeURIComponent(endpoint) +
    '&p=' + encodeURIComponent(config.project) +
    '&i=' + encodeURIComponent(instanceId);
}


// ============================================================
// 多任务批量执行 & Job 列表管理
// ============================================================

/** Job 列表 PropertiesService 键名 */
var MC_JOB_LIST_KEY = 'MC_JOB_LIST';

/** Job 列表最大条目数 */
var MAX_JOB_LIST_ENTRIES = 20;

/** Job 列表中 SQL 显示的最大长度 */
var MAX_JOB_SQL_DISPLAY_LENGTH = 200;

/**
 * 附加到已有 MaxCompute Instance
 *
 * 验证 instanceId 有效性，并返回基本信息供前端开始 polling。
 *
 * @param {string} instanceId - MaxCompute Instance ID
 * @return {Object} { instanceId, logviewUrl }
 */
function attachToInstance(instanceId) {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  instanceId = normalizeInstanceId_(instanceId);
  return {
    instanceId: instanceId,
    logviewUrl: buildLogviewUrl_(instanceId)
  };
}

/**
 * 获取 Job 列表
 * @return {Array<Object>} Job 记录数组
 */
function getJobList() {
  return readJobList_();
}

/**
 * 保存/更新单条 Job 记录
 *
 * 根据 record.id 查找：存在则更新，不存在则插入到头部。
 * 超出 MAX_JOB_LIST_ENTRIES 时裁剪尾部。
 *
 * @param {Object} record - Job 记录
 * @return {Object} { ok: true }
 */
function saveJobRecord(record) {
  if (!record || !record.id) {
    throw new Error('Job record must have an id');
  }

  var list = readJobList_();

  // 截断 SQL 显示长度
  if (record.sql && record.sql.length > MAX_JOB_SQL_DISPLAY_LENGTH) {
    record.sql = record.sql.substring(0, MAX_JOB_SQL_DISPLAY_LENGTH) + '...';
  }

  var found = false;
  for (var i = 0; i < list.length; i++) {
    if (list[i].id === record.id) {
      list[i] = record;
      found = true;
      break;
    }
  }

  if (!found) {
    list.unshift(record);
  }

  // 裁剪
  if (list.length > MAX_JOB_LIST_ENTRIES) {
    list = list.slice(0, MAX_JOB_LIST_ENTRIES);
  }

  writeJobList_(list);
  return { ok: true };
}

/**
 * 删除单条 Job 记录
 * @param {string} jobId - Job ID
 * @return {Object} { ok: true }
 */
function removeJobRecord(jobId) {
  var list = readJobList_();
  list = list.filter(function(item) { return item.id !== jobId; });
  writeJobList_(list);
  return { ok: true };
}

/**
 * 清空 Job 列表
 * @return {Object} { ok: true }
 */
function clearJobList() {
  writeJobList_([]);
  return { ok: true };
}

/**
 * 读取 Job 列表（内部函数）
 * @return {Array<Object>}
 */
function readJobList_() {
  var raw = PropertiesService.getUserProperties().getProperty(MC_JOB_LIST_KEY);
  if (!raw) return [];
  try {
    var parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    return [];
  }
}

/**
 * 写入 Job 列表（内部函数）
 * @param {Array<Object>} list
 */
function writeJobList_(list) {
  var props = PropertiesService.getUserProperties();
  if (!list || list.length === 0) {
    props.deleteProperty(MC_JOB_LIST_KEY);
    return;
  }
  props.setProperty(MC_JOB_LIST_KEY, JSON.stringify(list));
}
