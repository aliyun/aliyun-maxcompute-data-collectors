// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * 配置管理
 *
 * 用户属性说明：
 *   ALIYUN_ACCESS_KEY_ID       必填
 *   ALIYUN_ACCESS_KEY_SECRET   必填
 *   MC_SECURITY_TOKEN          可选（使用 STS 临时凭证时填写）
 *   MC_SECURITY_TOKEN_CLEARED  可选（用户明确清空 STS Token 时写入）
 *   MC_PROJECT                 必填
 *   MC_ENDPOINT                必填
 *
 * 兼容性：
 *   读取时用户属性优先，脚本属性兜底，兼容旧版本已保存的共享配置。
 *   保存时只写用户属性，避免多人使用同一插件时互相覆盖 AccessKey。
 */

var MAX_ACCESS_KEY_ID_LENGTH = 128;
var MAX_ACCESS_KEY_SECRET_LENGTH = 256;
var MAX_SECURITY_TOKEN_LENGTH = 4096;
var MAX_MC_PROJECT_LENGTH = 128;
var MAX_MC_ENDPOINT_LENGTH = 256;

function getMcConfig_() {
  var userProps = PropertiesService.getUserProperties().getProperties();
  var scriptProps = PropertiesService.getScriptProperties().getProperties();

  var cfg = {
    accessKeyId     : getConfigValue_(userProps, scriptProps, 'ALIYUN_ACCESS_KEY_ID'),
    accessKeySecret : getConfigValue_(userProps, scriptProps, 'ALIYUN_ACCESS_KEY_SECRET'),
    securityToken   : getSecurityTokenConfigValue_(userProps, scriptProps),
    project         : getConfigValue_(userProps, scriptProps, 'MC_PROJECT'),
    endpoint        : getConfigValue_(userProps, scriptProps, 'MC_ENDPOINT')
  };

  return cfg;
}

/**
 * 用户属性优先，脚本属性兜底。
 */
function getConfigValue_(userProps, scriptProps, key) {
  if (Object.prototype.hasOwnProperty.call(userProps, key)) {
    return userProps[key] || '';
  }
  return scriptProps[key] || '';
}

/**
 * Security Token 是可选字段。用户明确清空 Token 后，需要覆盖旧版脚本属性中的
 * 共享 Token 兜底，否则删除用户属性会导致下次读取时又读回旧值。
 */
function getSecurityTokenConfigValue_(userProps, scriptProps) {
  if (userProps['MC_SECURITY_TOKEN_CLEARED'] === 'true') {
    return '';
  }
  return getConfigValue_(userProps, scriptProps, 'MC_SECURITY_TOKEN');
}

/**
 * 获取设置页可展示的配置。
 *
 * 注意：不要把 AccessKey Secret / Security Token 明文返回到前端。
 */
function getMcConfigForUi() {
  var cfg = getMcConfig_();
  return {
    accessKeyId: cfg.accessKeyId,
    accessKeySecret: '',
    accessKeySecretConfigured: !!cfg.accessKeySecret,
    accessKeySecretMasked: maskValue_(cfg.accessKeySecret),
    securityToken: '',
    securityTokenConfigured: !!cfg.securityToken,
    securityTokenMasked: maskValue_(cfg.securityToken),
    project: cfg.project,
    endpoint: cfg.endpoint
  };
}

/**
 * 打开设置侧边栏
 */
function showSettings() {
  var html = HtmlService.createHtmlOutputFromFile('Settings')
    .setTitle('MaxCompute Settings')
    .setWidth(360);
  SpreadsheetApp.getUi().showSidebar(html);
}

/**
 * 保存配置（供 Settings.html 调用）
 */
function saveMcConfig(config) {
  var currentConfig = getMcConfig_();
  var effectiveConfig = mergeMcConfigForWrite_(config, currentConfig);
  assertRequiredMcConfig_(effectiveConfig);
  writeMcConfig_(effectiveConfig, !!(config && config.clearSecurityToken));
}

/**
 * 将前端传入配置与现有配置合并。
 *
 * Secret/Token 字段留空时默认沿用现有值，避免设置页为安全不回填密钥后，
 * 用户保存其他字段时误清空凭证。
 */
function mergeMcConfigForWrite_(config, currentConfig) {
  config = config || {};
  currentConfig = currentConfig || {};

  return {
    accessKeyId: trimOrFallback_(config.accessKeyId, currentConfig.accessKeyId),
    accessKeySecret: trimOrFallback_(config.accessKeySecret, currentConfig.accessKeySecret),
    securityToken: config.clearSecurityToken ? '' : trimOrFallback_(config.securityToken, currentConfig.securityToken),
    project: trimOrFallback_(config.project, currentConfig.project),
    endpoint: trimOrFallback_(config.endpoint, currentConfig.endpoint)
  };
}

/**
 * 写入完整配置。空值会删除对应属性。
 */
function writeMcConfig_(config, tokenCleared) {
  var props = PropertiesService.getUserProperties();

  setOrDeleteProperty_(props, 'ALIYUN_ACCESS_KEY_ID', config.accessKeyId);
  setOrDeleteProperty_(props, 'ALIYUN_ACCESS_KEY_SECRET', config.accessKeySecret);
  setOrDeleteProperty_(props, 'MC_PROJECT', config.project);
  setOrDeleteProperty_(props, 'MC_ENDPOINT', config.endpoint);
  setOrDeleteProperty_(props, 'MC_SECURITY_TOKEN', config.securityToken);
  setOrDeleteProperty_(props, 'MC_SECURITY_TOKEN_CLEARED',
    config.securityToken ? '' : (tokenCleared ? 'true' : ''));
}

/**
 * 恢复用户属性快照。
 */
function restoreUserMcConfigSnapshot_(snapshot) {
  snapshot = snapshot || {};
  writeMcConfig_({
    accessKeyId: snapshot['ALIYUN_ACCESS_KEY_ID'] || '',
    accessKeySecret: snapshot['ALIYUN_ACCESS_KEY_SECRET'] || '',
    project: snapshot['MC_PROJECT'] || '',
    endpoint: snapshot['MC_ENDPOINT'] || '',
    securityToken: snapshot['MC_SECURITY_TOKEN'] || ''
  }, snapshot['MC_SECURITY_TOKEN_CLEARED'] === 'true');
  setOrDeleteProperty_(PropertiesService.getUserProperties(),
    'MC_SECURITY_TOKEN_CLEARED',
    snapshot['MC_SECURITY_TOKEN_CLEARED'] || '');
}

/**
 * 设置或删除单个属性。
 */
function setOrDeleteProperty_(props, key, value) {
  value = value || '';
  if (value) {
    props.setProperty(key, value);
  } else {
    props.deleteProperty(key);
  }
}

/**
 * 取前端输入；空输入时沿用 fallback。
 */
function trimOrFallback_(value, fallback) {
  value = value === null || value === undefined ? '' : String(value).trim();
  return value || fallback || '';
}

/**
 * 校验必填配置。
 */
function assertRequiredMcConfig_(config) {
  if (!config.accessKeyId || !config.accessKeySecret || !config.project || !config.endpoint) {
    throw new Error('请填写所有必填项');
  }
  assertMcConfigLength_(config);
  assertValidMcEndpoint_(config.endpoint);
}

/**
 * 校验运行时可用配置。
 */
function assertUsableMcConfig_(config) {
  if (!config || !config.accessKeyId || !config.accessKeySecret || !config.project || !config.endpoint) {
    throw new Error('请先配置 AccessKey、Project 和 Endpoint（MaxCompute → 设置连接）');
  }
  assertMcConfigLength_(config);
  assertValidMcEndpoint_(config.endpoint);
}

/**
 * 校验配置字段长度，避免异常大输入进入 PropertiesService、签名或请求路径。
 */
function assertMcConfigLength_(config) {
  assertConfigFieldLength_('AccessKey ID', config.accessKeyId, MAX_ACCESS_KEY_ID_LENGTH);
  assertConfigFieldLength_('AccessKey Secret', config.accessKeySecret, MAX_ACCESS_KEY_SECRET_LENGTH);
  assertConfigFieldLength_('Project', config.project, MAX_MC_PROJECT_LENGTH);
  assertConfigFieldLength_('Endpoint', config.endpoint, MAX_MC_ENDPOINT_LENGTH);
  assertConfigFieldLength_('Security Token', config.securityToken || '', MAX_SECURITY_TOKEN_LENGTH);
}

function assertConfigFieldLength_(label, value, maxLength) {
  if (String(value || '').length > maxLength) {
    throw new Error(label + ' 长度超过限制（最多 ' + maxLength + ' 字符）');
  }
}

/**
 * 校验 MaxCompute Endpoint 格式。
 *
 * 配置页只允许保存 MaxCompute 公网 HTTPS API Endpoint，避免用户误填
 * http、控制台地址或非 MaxCompute 域名后在查询时才失败。
 */
function assertValidMcEndpoint_(endpoint) {
  endpoint = String(endpoint || '').trim();
  if (!/^https:\/\/service\.[a-z0-9-]+\.maxcompute\.aliyun\.com\/api$/i.test(endpoint)) {
    throw new Error('Endpoint 格式不正确，应为 https://service.{region}.maxcompute.aliyun.com/api');
  }
}

/**
 * 测试连接（供 Settings.html 调用）
 */
function testMcConnection(config) {
  var lang = getUserLanguage();
  var isZh = lang === 'zh';
  var originalUserConfig = PropertiesService.getUserProperties().getProperties();
  var currentConfig = getMcConfig_();
  var effectiveConfig = mergeMcConfigForWrite_(config, currentConfig);

  if (!effectiveConfig.accessKeyId || !effectiveConfig.accessKeySecret || !effectiveConfig.project || !effectiveConfig.endpoint) {
    return { success: false, message: isZh ? '请填写所有必填项' : 'Please fill all required fields' };
  }
  try {
    assertMcConfigLength_(effectiveConfig);
    assertValidMcEndpoint_(effectiveConfig.endpoint);
  } catch (e) {
    return { success: false, message: e.message };
  }

  // 临时保存配置用于测试，结束后无论成功失败都恢复原配置。
  writeMcConfig_(effectiveConfig, !!(config && config.clearSecurityToken));
  try {
    return testConnection();
  } finally {
    restoreUserMcConfigSnapshot_(originalUserConfig);
  }
}

function maskValue_(val) {
  if (!val) return '';
  if (val.length <= 6) return '****';
  return val.substring(0, 4) + '****' + val.substring(val.length - 4);
}


// ============================================================
// OSS 配置管理
// ============================================================

var MAX_OSS_ACCESS_KEY_ID_LENGTH = 128;
var MAX_OSS_ACCESS_KEY_SECRET_LENGTH = 256;
var MAX_OSS_SECURITY_TOKEN_LENGTH = 4096;
var MAX_OSS_BUCKET_LENGTH = 63;
var MAX_OSS_ENDPOINT_LENGTH = 128;

/**
 * 读取 OSS 配置。
 */
function getOssConfig_() {
  var userProps = PropertiesService.getUserProperties().getProperties();
  var scriptProps = PropertiesService.getScriptProperties().getProperties();

  return {
    accessKeyId     : getConfigValue_(userProps, scriptProps, 'OSS_ACCESS_KEY_ID'),
    accessKeySecret : getConfigValue_(userProps, scriptProps, 'OSS_ACCESS_KEY_SECRET'),
    securityToken   : getOssSecurityTokenConfigValue_(userProps, scriptProps),
    endpoint        : getConfigValue_(userProps, scriptProps, 'OSS_ENDPOINT'),
    bucket          : getConfigValue_(userProps, scriptProps, 'OSS_BUCKET')
  };
}

/**
 * OSS Security Token 读取（与 MC 同逻辑）。
 */
function getOssSecurityTokenConfigValue_(userProps, scriptProps) {
  if (userProps['OSS_SECURITY_TOKEN_CLEARED'] === 'true') {
    return '';
  }
  return getConfigValue_(userProps, scriptProps, 'OSS_SECURITY_TOKEN');
}

/**
 * 获取 OSS 配置（前端展示用，脱敏）。
 */
function getOssConfigForUi() {
  var cfg = getOssConfig_();
  return {
    accessKeyId: cfg.accessKeyId,
    accessKeySecret: '',
    accessKeySecretConfigured: !!cfg.accessKeySecret,
    accessKeySecretMasked: maskValue_(cfg.accessKeySecret),
    securityToken: '',
    securityTokenConfigured: !!cfg.securityToken,
    securityTokenMasked: maskValue_(cfg.securityToken),
    endpoint: cfg.endpoint,
    bucket: cfg.bucket
  };
}

/**
 * 保存 OSS 配置（供 Settings.html 调用）。
 */
function saveOssConfig(config) {
  var currentConfig = getOssConfig_();
  var effectiveConfig = mergeOssConfigForWrite_(config, currentConfig);
  assertRequiredOssConfig_(effectiveConfig);
  writeOssConfig_(effectiveConfig, !!(config && config.clearSecurityToken));
}

/**
 * 合并前端 OSS 配置。
 */
function mergeOssConfigForWrite_(config, currentConfig) {
  config = config || {};
  currentConfig = currentConfig || {};

  return {
    accessKeyId: trimOrFallback_(config.accessKeyId, currentConfig.accessKeyId),
    accessKeySecret: trimOrFallback_(config.accessKeySecret, currentConfig.accessKeySecret),
    securityToken: config.clearSecurityToken ? '' : trimOrFallback_(config.securityToken, currentConfig.securityToken),
    endpoint: trimOrFallback_(config.endpoint, currentConfig.endpoint),
    bucket: trimOrFallback_(config.bucket, currentConfig.bucket)
  };
}

/**
 * 写入 OSS 配置。
 */
function writeOssConfig_(config, tokenCleared) {
  var props = PropertiesService.getUserProperties();

  setOrDeleteProperty_(props, 'OSS_ACCESS_KEY_ID', config.accessKeyId);
  setOrDeleteProperty_(props, 'OSS_ACCESS_KEY_SECRET', config.accessKeySecret);
  setOrDeleteProperty_(props, 'OSS_ENDPOINT', config.endpoint);
  setOrDeleteProperty_(props, 'OSS_BUCKET', config.bucket);
  setOrDeleteProperty_(props, 'OSS_SECURITY_TOKEN', config.securityToken);
  setOrDeleteProperty_(props, 'OSS_SECURITY_TOKEN_CLEARED',
    config.securityToken ? '' : (tokenCleared ? 'true' : ''));
}

/**
 * 校验 OSS 必填配置。
 */
function assertRequiredOssConfig_(config) {
  if (!config.accessKeyId || !config.accessKeySecret || !config.endpoint || !config.bucket) {
    throw new Error('请填写 OSS 所有必填项（AccessKey ID/Secret、Endpoint、Bucket）');
  }
  assertOssConfigLength_(config);
  assertValidOssEndpoint_(config.endpoint);
  assertValidOssBucket_(config.bucket);
}

/**
 * 校验运行时可用 OSS 配置。
 */
function assertUsableOssConfig_(config) {
  if (!config || !config.accessKeyId || !config.accessKeySecret || !config.endpoint || !config.bucket) {
    throw new Error('请先配置 OSS 连接（MaxCompute → 设置连接 → OSS Export 区域）');
  }
  assertOssConfigLength_(config);
  assertValidOssEndpoint_(config.endpoint);
  assertValidOssBucket_(config.bucket);
}

/**
 * 校验 OSS 配置字段长度。
 */
function assertOssConfigLength_(config) {
  assertConfigFieldLength_('OSS AccessKey ID', config.accessKeyId, MAX_OSS_ACCESS_KEY_ID_LENGTH);
  assertConfigFieldLength_('OSS AccessKey Secret', config.accessKeySecret, MAX_OSS_ACCESS_KEY_SECRET_LENGTH);
  assertConfigFieldLength_('OSS Bucket', config.bucket, MAX_OSS_BUCKET_LENGTH);
  assertConfigFieldLength_('OSS Endpoint', config.endpoint, MAX_OSS_ENDPOINT_LENGTH);
  assertConfigFieldLength_('OSS Security Token', config.securityToken || '', MAX_OSS_SECURITY_TOKEN_LENGTH);
}

/**
 * 校验 OSS Endpoint 格式（仅允许 oss-{region} 格式）。
 */
function assertValidOssEndpoint_(endpoint) {
  endpoint = String(endpoint || '').trim();
  if (!/^oss-[a-z0-9-]+$/.test(endpoint)) {
    throw new Error('OSS Endpoint 格式不正确，应为 oss-{region}（如 oss-cn-hangzhou）');
  }
}

/**
 * 校验 OSS Bucket 名称合法性。
 */
function assertValidOssBucket_(bucket) {
  bucket = String(bucket || '').trim();
  if (!/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(bucket)) {
    throw new Error('OSS Bucket 名称不合法（3-63 字符，小写字母/数字/短横线，不能以短横线开头或结尾）');
  }
}

/**
 * 测试 OSS 连接。
 */
function testOssConnection(config) {
  var lang = getUserLanguage();
  var isZh = lang === 'zh';
  var originalUserProps = PropertiesService.getUserProperties().getProperties();
  var currentConfig = getOssConfig_();
  var effectiveConfig = mergeOssConfigForWrite_(config, currentConfig);

  if (!effectiveConfig.accessKeyId || !effectiveConfig.accessKeySecret || !effectiveConfig.endpoint || !effectiveConfig.bucket) {
    return { success: false, message: isZh ? '请填写 OSS 所有必填项' : 'Please fill all required OSS fields' };
  }
  try {
    assertOssConfigLength_(effectiveConfig);
    assertValidOssEndpoint_(effectiveConfig.endpoint);
    assertValidOssBucket_(effectiveConfig.bucket);
  } catch (e) {
    return { success: false, message: e.message };
  }

  // 临时写入配置测试
  writeOssConfig_(effectiveConfig, !!(config && config.clearSecurityToken));
  try {
    var response = ossFetch_({
      method: 'GET',
      region: effectiveConfig.endpoint,
      bucket: effectiveConfig.bucket,
      objectKey: '',
      accessKeyId: effectiveConfig.accessKeyId,
      accessKeySecret: effectiveConfig.accessKeySecret,
      securityToken: effectiveConfig.securityToken,
      contentType: ''
    });

    var code = response.getResponseCode();
    if (code === 200) {
      return { success: true, message: isZh ? 'OSS 连接成功' : 'OSS connection successful' };
    } else if (code === 404) {
      return { success: false, message: isZh ? 'Bucket 不存在' : 'Bucket not found' };
    } else if (code === 403) {
      return { success: false, message: isZh ?
        'HTTP 403 — 凭证无效或权限不足（需要 oss:GetBucket 权限）' :
        'HTTP 403 — Invalid credentials or insufficient permission (requires oss:GetBucket)' };
    } else {
      return { success: false, message: isZh ?
        'HTTP ' + code + ' — 未知错误' :
        'HTTP ' + code + ' — Unknown error' };
    }
  } catch (e) {
    return { success: false, message: e.message };
  } finally {
    // 恢复原配置
    restoreUserOssConfigSnapshot_(originalUserProps);
  }
}

/**
 * 恢复 OSS 用户属性快照。
 */
function restoreUserOssConfigSnapshot_(snapshot) {
  snapshot = snapshot || {};
  writeOssConfig_({
    accessKeyId: snapshot['OSS_ACCESS_KEY_ID'] || '',
    accessKeySecret: snapshot['OSS_ACCESS_KEY_SECRET'] || '',
    endpoint: snapshot['OSS_ENDPOINT'] || '',
    bucket: snapshot['OSS_BUCKET'] || '',
    securityToken: snapshot['OSS_SECURITY_TOKEN'] || ''
  }, snapshot['OSS_SECURITY_TOKEN_CLEARED'] === 'true');
  setOrDeleteProperty_(PropertiesService.getUserProperties(),
    'OSS_SECURITY_TOKEN_CLEARED',
    snapshot['OSS_SECURITY_TOKEN_CLEARED'] || '');
}

/**
 * 读取 OSS 导出偏好设置。
 */
function getExportPreferences() {
  var props = PropertiesService.getUserProperties();
  return {
    prefix: props.getProperty('OSS_EXPORT_PREFIX') || '',
    template: props.getProperty('OSS_EXPORT_TEMPLATE') || '{prefix}{sheet}_{date}.csv'
  };
}

/**
 * 保存 OSS 导出偏好设置。
 */
function saveExportPreferences(prefix, template) {
  var props = PropertiesService.getUserProperties();
  props.setProperty('OSS_EXPORT_PREFIX', String(prefix || ''));
  props.setProperty('OSS_EXPORT_TEMPLATE', String(template || '{prefix}{sheet}_{date}.csv'));
}
