// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * OSS 导出器
 *
 * 将 Google Sheet 数据导出为 CSV 并上传到阿里云 OSS。
 *
 * 流程：
 * 1. 读取指定 Sheet 的数据 (getDisplayValues)
 * 2. 转换为 RFC 4180 CSV 格式（含 UTF-8 BOM）
 * 3. PUT 到 OSS 指定 Object Key
 */

var MAX_EXPORT_SIZE_BYTES = 40 * 1024 * 1024; // 40MB 软限制（UrlFetchApp 50MB 硬限制）
var CSV_BOM = '﻿';

// ============================================================
// 公开接口
// ============================================================

/**
 * 导出 Sheet 数据为 CSV 并上传到 OSS。
 *
 * @param {string} sheetName  - 源 Sheet 名称
 * @param {string} objectKey  - OSS Object Key（路径）
 * @return {Object} { success, objectKey, rows, cols, sizeBytes }
 */
function exportSheetToCsv(sheetName, objectKey) {
  // 校验 OSS 配置
  var ossConfig = getOssConfig_();
  assertUsableOssConfig_(ossConfig);

  // 校验参数
  sheetName = String(sheetName || '').trim();
  objectKey = String(objectKey || '').trim();
  if (!sheetName) {
    throw new Error('请选择要导出的 Sheet');
  }
  if (!objectKey) {
    throw new Error('请填写 OSS Object Key（文件路径）');
  }
  validateObjectKey_(objectKey);

  Logger.log('[OssExporter] exportSheetToCsv sheetName=' + sheetName +
    ' objectKey=' + truncateLogValue_(objectKey, 80));

  // 读取 Sheet 数据
  var values = readSheetValues_(sheetName);
  if (!values || values.length === 0) {
    throw new Error('Sheet "' + sheetName + '" 为空，无数据可导出');
  }

  // 转换为 CSV
  var csvContent = convertValuesToCsv_(values);

  // 检查大小
  var csvBlob = Utilities.newBlob(csvContent, 'text/csv; charset=utf-8');
  var sizeBytes = csvBlob.getBytes().length;

  if (sizeBytes > MAX_EXPORT_SIZE_BYTES) {
    throw new Error('CSV 文件大小（' + formatBytes_(sizeBytes) + '）超过 40MB 限制，请减少数据量');
  }

  Logger.log('[OssExporter] CSV generated: rows=' + values.length +
    ' cols=' + (values[0] ? values[0].length : 0) +
    ' size=' + formatBytes_(sizeBytes));

  // 上传到 OSS
  uploadCsvToOss_(csvBlob.getBytes(), objectKey, ossConfig);

  return {
    success: true,
    objectKey: objectKey,
    rows: values.length,
    cols: values[0] ? values[0].length : 0,
    sizeBytes: sizeBytes,
    sizeFormatted: formatBytes_(sizeBytes)
  };
}

/**
 * 获取当前 Spreadsheet 所有 Sheet 名称列表。
 *
 * @return {string[]}
 */
function getExportableSheets() {
  var sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
  var names = [];
  for (var i = 0; i < sheets.length; i++) {
    names.push(sheets[i].getName());
  }
  return names;
}

/**
 * 获取 OSS 导出状态（是否已配置）。
 *
 * @return {Object} { configured: boolean }
 */
function getOssExportStatus() {
  var cfg = getOssConfig_();
  var configured = !!(cfg.accessKeyId && cfg.accessKeySecret && cfg.endpoint && cfg.bucket);
  return { configured: configured };
}


// ============================================================
// 内部函数
// ============================================================

/**
 * 读取 Sheet 数据（显示值）。
 *
 * @param {string} sheetName
 * @return {string[][]}
 */
function readSheetValues_(sheetName) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    throw new Error('找不到 Sheet: ' + sheetName);
  }

  var lastRow = sheet.getLastRow();
  var lastCol = sheet.getLastColumn();
  if (lastRow === 0 || lastCol === 0) {
    return [];
  }

  return sheet.getRange(1, 1, lastRow, lastCol).getDisplayValues();
}

/**
 * 将二维数组转换为 RFC 4180 CSV 字符串（含 BOM）。
 *
 * @param {string[][]} values
 * @return {string}
 */
function convertValuesToCsv_(values) {
  var lines = [];
  for (var i = 0; i < values.length; i++) {
    var row = values[i];
    var fields = [];
    for (var j = 0; j < row.length; j++) {
      fields.push(escapeCsvField_(row[j]));
    }
    lines.push(fields.join(','));
  }
  return CSV_BOM + lines.join('
');
}

/**
 * RFC 4180 CSV 字段转义。
 *
 * 如果字段包含逗号、双引号、换行符，则用双引号包裹，
 * 内部的双引号转义为两个双引号。
 *
 * @param {*} value
 * @return {string}
 */
function escapeCsvField_(value) {
  var str = (value === null || value === undefined) ? '' : String(value);
  if (str.indexOf(',') !== -1 || str.indexOf('"') !== -1 ||
      str.indexOf('
') !== -1 || str.indexOf('') !== -1) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

/**
 * 上传 CSV 到 OSS。
 *
 * @param {byte[]} csvBytes    - CSV 文件字节数组
 * @param {string} objectKey   - OSS Object Key
 * @param {Object} ossConfig   - OSS 配置
 */
function uploadCsvToOss_(csvBytes, objectKey, ossConfig) {
  var response = ossFetch_({
    method: 'PUT',
    region: ossConfig.endpoint,
    bucket: ossConfig.bucket,
    objectKey: objectKey,
    accessKeyId: ossConfig.accessKeyId,
    accessKeySecret: ossConfig.accessKeySecret,
    securityToken: ossConfig.securityToken,
    body: csvBytes,
    contentType: 'text/csv; charset=utf-8'
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    var body = response.getContentText();
    var errorMsg = parseOssErrorMessage_(body);
    throw new Error('OSS 上传失败 (HTTP ' + code + '): ' + errorMsg);
  }
}

/**
 * 从 OSS XML 错误响应中提取错误信息。
 *
 * @param {string} xml
 * @return {string}
 */
function parseOssErrorMessage_(xml) {
  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();
    var code = root.getChildText('Code') || '';
    var message = root.getChildText('Message') || '';
    return code + (message ? ': ' + message : '');
  } catch (e) {
    // 非 XML 响应，截断返回
    return String(xml || '').substring(0, 200);
  }
}

/**
 * 校验 OSS Object Key 合法性。
 *
 * @param {string} key
 */
function validateObjectKey_(key) {
  if (!key || key.length > 1023) {
    throw new Error('Object Key 长度须为 1-1023 字符');
  }
  if (key.indexOf('\') !== -1) {
    throw new Error('Object Key 不能包含反斜杠 (\)');
  }
  if (/^\//.test(key)) {
    throw new Error('Object Key 不能以 / 开头');
  }
}

/**
 * 格式化字节数为人类可读格式。
 *
 * @param {number} bytes
 * @return {string}
 */
function formatBytes_(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}
