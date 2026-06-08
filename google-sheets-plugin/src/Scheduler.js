// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * MaxCompute 定时调度器
 *
 * 使用 Google Apps Script 的 ScriptApp.newTrigger() 创建定时触发器，
 * 定期检查到期的调度任务并执行 SQL 查询。
 *
 * 关键设计：
 * - 单个 Trigger 处理所有调度任务
 * - Instance-Attach 模式跨 trigger fire 处理长时间运行的查询
 * - 5 分钟安全阈值（Apps Script 6 分钟执行限制）
 */

// ============================================================
// 常量
// ============================================================

/** 调度列表 PropertiesService 键名 */
var MC_SCHEDULE_LIST_KEY = 'MC_SCHEDULE_LIST';

/** 调度运行时状态 PropertiesService 键名 */
var MC_SCHEDULE_STATE_KEY = 'MC_SCHEDULE_STATE';

/** 已安装 Trigger ID PropertiesService 键名 */
var MC_TRIGGER_ID_KEY = 'MC_TRIGGER_ID';

/** 最大调度数 */
var MAX_SCHEDULES = 10;

/** Trigger 安全运行边际（毫秒），留 1 分钟给收尾操作 */
var TRIGGER_SAFETY_MARGIN_MS = 60 * 1000;

/** Trigger 最大运行时长（毫秒），含安全边际 = 5 分钟 */
var MAX_TRIGGER_RUNTIME_MS = 5 * 60 * 1000;

/** 调度内轮询单个 instance 的最大等待（毫秒）*/
var SCHEDULE_POLL_WAIT_MS = 8000;


// ============================================================
// Trigger 入口
// ============================================================

/**
 * 定时 Trigger 回调入口 - 由 ScriptApp.newTrigger() 注册
 *
 * 每次 fire 的工作流：
 * 1. 读取 MC_SCHEDULE_LIST，找到所有 enabled 且到期的 schedule
 * 2. 读取 MC_SCHEDULE_STATE，检查 in-flight instances
 * 3. 对每个到期 schedule：
 *    - 无 in-flight → 提交 SQL tasks，写入 state
 *    - 有 in-flight → 轮询状态，成功则写入 Sheet
 * 4. 更新 lastRunAt / lastRunStatus
 * 5. 总运行时间 < 5 分钟
 */
function scheduledJobFire_() {
  var startTime = Date.now();
  var schedules = readScheduleList_();
  var state = readScheduleState_();
  var now = Date.now();
  var changed = false;

  for (var i = 0; i < schedules.length; i++) {
    // 检查剩余时间
    if (Date.now() - startTime > MAX_TRIGGER_RUNTIME_MS) {
      Logger.log('[Scheduler] Time budget exceeded, saving state and exiting');
      break;
    }

    var schedule = schedules[i];
    if (!schedule.enabled) continue;

    // 检查是否到期
    var nextRun = schedule.nextRunAt || 0;
    var hasInFlight = hasInFlightInstances_(state, schedule.id);

    if (!hasInFlight && nextRun > now) continue;

    try {
      if (!hasInFlight) {
        // 提交新的 SQL 任务
        submitScheduledTasks_(schedule, state);
        changed = true;
      }

      // 轮询/写入 in-flight instances
      var allDone = pollScheduledInstances_(schedule, state, startTime);

      if (allDone) {
        // 所有任务完成，更新调度状态
        schedule.lastRunAt = now;
        schedule.lastRunStatus = 'success';
        schedule.nextRunAt = calculateNextRunAt_(schedule, now);
        clearScheduleState_(state, schedule.id);
        changed = true;
      }
    } catch (e) {
      Logger.log('[Scheduler] Error processing schedule ' + schedule.id + ': ' + e.message);
      schedule.lastRunAt = now;
      schedule.lastRunStatus = 'failed';
      schedule.nextRunAt = calculateNextRunAt_(schedule, now);
      clearScheduleState_(state, schedule.id);
      changed = true;
    }
  }

  if (changed) {
    writeScheduleList_(schedules);
    writeScheduleState_(state);
  }
}


// ============================================================
// 调度任务提交 & 轮询
// ============================================================

/**
 * 提交调度内的所有 SQL 任务
 */
function submitScheduledTasks_(schedule, state) {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  var tasks = schedule.tasks || [];
  for (var j = 0; j < tasks.length; j++) {
    var task = tasks[j];
    var stateKey = schedule.id + '_' + j;

    try {
      var ctx = {
        user: getCurrentUserAuditKey_(),
        spreadsheetName: '',
        spreadsheetId: schedule.spreadsheetId || '',
        targetSheet: task.targetSheet || ''
      };

      // 获取 spreadsheet 信息
      try {
        var ss = schedule.spreadsheetId
          ? SpreadsheetApp.openById(schedule.spreadsheetId)
          : SpreadsheetApp.getActiveSpreadsheet();
        if (ss) {
          ctx.spreadsheetName = ss.getName();
          ctx.spreadsheetId = ss.getId();
        }
      } catch (e) {}

      var result = submitSqlJobOnly_(task.sql.trim(), ctx);

      if (result.sync) {
        // 同步完成
        var data = prepareResultData_(result.result, MAX_RESULT_ROWS);
        writePreparedResultToSheet_(data, task.targetSheet, schedule.spreadsheetId);
        state[stateKey] = {
          instanceId: '',
          targetSheet: task.targetSheet,
          submittedAt: Date.now(),
          status: 'done'
        };
        // 写入 Job 记录 - 已完成
        saveJobRecord({
          id: result.instanceId || ('sched_sync_' + Date.now() + '_' + j),
          instanceId: result.instanceId || '',
          mode: 'sql',
          sql: task.sql.trim().substring(0, 200),
          targetSheet: task.targetSheet,
          status: 'success',
          error: null,
          logviewUrl: result.logviewUrl || null,
          createdAt: Date.now(),
          source: 'schedule',
          scheduleName: schedule.name || schedule.id,
          summary: { row_count: data.row_count }
        });
      } else {
        state[stateKey] = {
          instanceId: result.instanceId,
          targetSheet: task.targetSheet,
          submittedAt: Date.now(),
          status: 'polling'
        };
        // 写入 Job 记录 - 运行中
        saveJobRecord({
          id: result.instanceId,
          instanceId: result.instanceId,
          mode: 'sql',
          sql: task.sql.trim().substring(0, 200),
          targetSheet: task.targetSheet,
          status: 'running',
          error: null,
          logviewUrl: result.logviewUrl || null,
          createdAt: Date.now(),
          source: 'schedule',
          scheduleName: schedule.name || schedule.id,
          summary: null
        });
      }
    } catch (e) {
      state[stateKey] = {
        instanceId: '',
        targetSheet: task.targetSheet,
        submittedAt: Date.now(),
        status: 'failed',
        error: e.message
      };
      // 写入 Job 记录 - 失败
      saveJobRecord({
        id: 'sched_fail_' + Date.now() + '_' + j,
        instanceId: '',
        mode: 'sql',
        sql: task.sql.trim().substring(0, 200),
        targetSheet: task.targetSheet,
        status: 'failed',
        error: e.message,
        logviewUrl: null,
        createdAt: Date.now(),
        source: 'schedule',
        scheduleName: schedule.name || schedule.id,
        summary: null
      });
    }
  }
}

/**
 * 轮询调度内所有 in-flight instances
 * @return {boolean} 是否全部完成
 */
function pollScheduledInstances_(schedule, state, startTime) {
  var tasks = schedule.tasks || [];
  var allDone = true;

  for (var j = 0; j < tasks.length; j++) {
    var stateKey = schedule.id + '_' + j;
    var entry = state[stateKey];
    if (!entry) continue;

    if (entry.status === 'done' || entry.status === 'failed') continue;

    // 检查剩余时间
    if (Date.now() - startTime > MAX_TRIGGER_RUNTIME_MS) {
      allDone = false;
      break;
    }

    if (entry.status === 'polling' && entry.instanceId) {
      try {
        var progress = getJobProgress_(entry.instanceId);

        if (progress.instanceTerminated) {
          if (progress.taskStatus === 'Success') {
            // 写入结果
            var result = getJobResult_(entry.instanceId);
            var data = prepareResultData_(result, MAX_RESULT_ROWS);
            writePreparedResultToSheet_(data, entry.targetSheet, schedule.spreadsheetId);
            entry.status = 'done';
            // 更新 Job 记录状态
            saveJobRecord({
              id: entry.instanceId,
              instanceId: entry.instanceId,
              status: 'success',
              summary: { row_count: data.row_count }
            });
          } else {
            entry.status = 'failed';
            entry.error = progress.errorSummary || 'Task ' + progress.taskStatus;
            // 更新 Job 记录状态
            saveJobRecord({
              id: entry.instanceId,
              instanceId: entry.instanceId,
              status: 'failed',
              error: entry.error
            });
          }
        } else {
          allDone = false;
        }
      } catch (e) {
        entry.status = 'failed';
        entry.error = e.message;
        // 更新 Job 记录状态
        saveJobRecord({
          id: entry.instanceId,
          instanceId: entry.instanceId,
          status: 'failed',
          error: e.message
        });
      }
    }
  }

  return allDone;
}


// ============================================================
// 状态辅助
// ============================================================

/**
 * 检查某个 schedule 是否有 in-flight instances
 */
function hasInFlightInstances_(state, scheduleId) {
  var prefix = scheduleId + '_';
  for (var key in state) {
    if (key.indexOf(prefix) === 0) {
      var entry = state[key];
      if (entry.status === 'polling') return true;
    }
  }
  return false;
}

/**
 * 清除某个 schedule 的所有运行时状态
 */
function clearScheduleState_(state, scheduleId) {
  var prefix = scheduleId + '_';
  for (var key in state) {
    if (key.indexOf(prefix) === 0) {
      delete state[key];
    }
  }
}


// ============================================================
// 调度频率计算
// ============================================================

/**
 * 计算下次执行时间
 *
 * @param {Object} schedule - 调度配置
 * @param {number} now - 当前时间戳
 * @return {number} 下次执行时间戳
 */
function calculateNextRunAt_(schedule, now) {
  var freqType = schedule.frequencyType || schedule.type;

  if (freqType === 'everyNHours') {
    var intervalMs = (schedule.frequencyValue || schedule.interval || 1) * 3600000;
    return now + intervalMs;
  }

  if (freqType === 'dailyAtHour') {
    var hour = schedule.frequencyValue || schedule.hour || 0;
    var d = new Date(now);
    var next = new Date(d.getFullYear(), d.getMonth(), d.getDate(), hour, 0, 0, 0);
    if (next.getTime() <= now) {
      next.setDate(next.getDate() + 1);
    }
    return next.getTime();
  }

  return now + 3600000;
}


// ============================================================
// Trigger 安装 / 卸载
// ============================================================

/**
 * 安装定时 Trigger（每小时触发一次）
 * @return {Object} { triggerId, installed }
 */
function installScheduleTrigger() {
  // 检查是否已安装
  var existingId = getInstalledTriggerId_();
  if (existingId) {
    return { triggerId: existingId, installed: true, alreadyExists: true };
  }

  var trigger = ScriptApp.newTrigger('scheduledJobFire_')
    .timeBased()
    .everyHours(1)
    .create();

  var triggerId = trigger.getUniqueId();
  PropertiesService.getUserProperties().setProperty(MC_TRIGGER_ID_KEY, triggerId);

  return { triggerId: triggerId, installed: true };
}

/**
 * 卸载定时 Trigger
 * @return {Object} { ok: true }
 */
function uninstallScheduleTrigger() {
  var props = PropertiesService.getUserProperties();
  var savedId = props.getProperty(MC_TRIGGER_ID_KEY);

  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'scheduledJobFire_') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  props.deleteProperty(MC_TRIGGER_ID_KEY);
  return { ok: true };
}

/**
 * 获取已安装 Trigger ID（验证 trigger 仍然存在）
 * @return {string|null}
 */
function getInstalledTriggerId_() {
  var savedId = PropertiesService.getUserProperties().getProperty(MC_TRIGGER_ID_KEY);
  if (!savedId) return null;

  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getUniqueId() === savedId) {
      return savedId;
    }
  }

  // Trigger 不存在，清理
  PropertiesService.getUserProperties().deleteProperty(MC_TRIGGER_ID_KEY);
  return null;
}

/**
 * 获取 Trigger 状态
 * @return {Object} { installed, triggerId }
 */
function getScheduleTriggerStatus() {
  var triggerId = getInstalledTriggerId_();
  return {
    installed: !!triggerId,
    triggerId: triggerId || ''
  };
}


// ============================================================
// Schedule CRUD（PropertiesService）
// ============================================================

/**
 * 读取调度列表
 * @return {Array<Object>}
 */
function readScheduleList_() {
  var raw = PropertiesService.getUserProperties().getProperty(MC_SCHEDULE_LIST_KEY);
  if (!raw) return [];
  try {
    var parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    return [];
  }
}

/**
 * 写入调度列表
 * @param {Array<Object>} list
 */
function writeScheduleList_(list) {
  var props = PropertiesService.getUserProperties();
  if (!list || list.length === 0) {
    props.deleteProperty(MC_SCHEDULE_LIST_KEY);
    return;
  }
  props.setProperty(MC_SCHEDULE_LIST_KEY, JSON.stringify(list));
}

/**
 * 读取调度运行时状态
 * @return {Object}
 */
function readScheduleState_() {
  var raw = PropertiesService.getUserProperties().getProperty(MC_SCHEDULE_STATE_KEY);
  if (!raw) return {};
  try {
    var parsed = JSON.parse(raw);
    return (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) ? parsed : {};
  } catch (e) {
    return {};
  }
}

/**
 * 写入调度运行时状态
 * @param {Object} state
 */
function writeScheduleState_(state) {
  var props = PropertiesService.getUserProperties();
  if (!state || Object.keys(state).length === 0) {
    props.deleteProperty(MC_SCHEDULE_STATE_KEY);
    return;
  }
  props.setProperty(MC_SCHEDULE_STATE_KEY, JSON.stringify(state));
}


// ============================================================
// Schedule 公开 API（供侧边栏调用）
// ============================================================

/**
 * 获取调度列表
 * @return {Array<Object>}
 */
function getScheduleList() {
  return readScheduleList_();
}

/**
 * 保存/更新调度配置
 *
 * @param {Object} schedule - 调度配置
 * @return {Object} { ok: true, id: string }
 */
function saveSchedule(schedule) {
  if (!schedule) {
    throw new Error('Schedule is required');
  }

  var list = readScheduleList_();

  // 生成或使用已有 ID
  if (!schedule.id) {
    schedule.id = 'sched_' + Date.now() + '_' + Math.random().toString(36).substr(2, 6);
    schedule.createdAt = Date.now();
    schedule.nextRunAt = calculateNextRunAt_(schedule, Date.now());
    // 记录关联的 Spreadsheet ID，供 Trigger 使用（Trigger 无 active spreadsheet）
    try {
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      if (ss) schedule.spreadsheetId = ss.getId();
    } catch (e) {}
    list.unshift(schedule);
  } else {
    var found = false;
    for (var i = 0; i < list.length; i++) {
      if (list[i].id === schedule.id) {
        var existing = list[i];
        schedule.createdAt = existing.createdAt;
        schedule.spreadsheetId = existing.spreadsheetId;
        schedule.lastRunAt = existing.lastRunAt;
        schedule.lastRunStatus = existing.lastRunStatus;
        schedule.nextRunAt = calculateNextRunAt_(schedule, Date.now());
        list[i] = schedule;
        found = true;
        break;
      }
    }
    if (!found) {
      schedule.createdAt = Date.now();
      schedule.nextRunAt = calculateNextRunAt_(schedule, Date.now());
      try {
        var ss2 = SpreadsheetApp.getActiveSpreadsheet();
        if (ss2) schedule.spreadsheetId = ss2.getId();
      } catch (e2) {}
      list.unshift(schedule);
    }
  }

  if (list.length > MAX_SCHEDULES) {
    list = list.slice(0, MAX_SCHEDULES);
  }

  writeScheduleList_(list);
  return { ok: true, id: schedule.id };
}

/**
 * 删除调度
 *
 * @param {string} scheduleId
 * @return {Object} { ok: true }
 */
function deleteSchedule(scheduleId) {
  var list = readScheduleList_();
  list = list.filter(function(item) { return item.id !== scheduleId; });
  writeScheduleList_(list);

  // 清理运行时状态
  var state = readScheduleState_();
  clearScheduleState_(state, scheduleId);
  writeScheduleState_(state);

  // 无任何 enabled schedule 时卸载 Trigger
  var hasEnabled = list.some(function(item) { return item.enabled; });
  if (!hasEnabled) {
    uninstallScheduleTrigger();
  }

  return { ok: true };
}

/**
 * 启用/禁用调度
 *
 * @param {string} scheduleId
 * @param {boolean} enabled
 * @return {Object} { ok: true }
 */
function toggleSchedule(scheduleId, enabled) {
  var list = readScheduleList_();
  for (var i = 0; i < list.length; i++) {
    if (list[i].id === scheduleId) {
      list[i].enabled = !!enabled;
      break;
    }
  }
  writeScheduleList_(list);

  // 自动管理 Trigger：无任何 enabled schedule 时卸载，有则确保安装
  var hasEnabled = list.some(function(item) { return item.enabled; });
  if (hasEnabled) {
    installScheduleTrigger();
  } else {
    uninstallScheduleTrigger();
  }

  return { ok: true };
}
