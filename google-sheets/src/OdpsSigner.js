// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * ODPS Signature V1 签名实现
 *
 * 用于 MaxCompute Instance Job API 的签名认证。
 * 签名格式：Authorization: ODPS <AccessId>:<Signature>
 *
 * 参考文档：
 * - Instance Job API: https://help.aliyun.com/document_detail/27985.html
 * - 签名机制: https://help.aliyun.com/document_detail/34951.html
 */

// ============================================================
// 公开接口
// ============================================================

/** 插件发出的外部请求统一 User-Agent */
var MC_GSHEET_PLUGIN_USER_AGENT = 'Google Sheet Plugin';

/**
 * ODPS V1 签名请求
 *
 * @param {Object} p
 * @param {string} p.method          - HTTP 方法 GET/POST/PUT
 * @param {string} p.host            - 主机名，如 service.cn-hangzhou.maxcompute.aliyun.com
 * @param {string} p.pathname        - URL 路径，如 /projects/my_project/instances
 * @param {string} p.accessKeyId     - AccessKey ID
 * @param {string} p.accessKeySecret - AccessKey Secret
 * @param {string} [p.securityToken] - STS Token（可选）
 * @param {string} [p.body]          - 请求体（XML 字符串）
 * @param {Object} [p.query]         - 查询参数 { key: value }
 * @return {HTTPResponse}
 */
function odpsFetch_(p) {
  var method = (p.method || 'GET').toUpperCase();
  var date = Utilities.formatDate(new Date(), 'GMT', "EEE, dd MMM yyyy HH:mm:ss 'GMT'");

  // 构造 headers
  var headers = {
    'Date': date,
    'Content-MD5': '',
    'Content-Type': 'application/xml',
    'User-Agent': MC_GSHEET_PLUGIN_USER_AGENT
  };

  // STS Token
  if (p.securityToken) {
    headers['x-odps-security-token'] = p.securityToken;
  }

  // 添加 curr_project 参数
  var query = copyQueryParams_(p.query);
  if (p.project) {
    query['curr_project'] = p.project;
  }

  // 构建 CanonicalizedResource
  var resource = buildCanonicalResource_(p.pathname, query);

  // 构建 StringToSign
  var stringToSign = buildCanonicalString_(method, headers, resource, 'x-odps-');

  // 计算签名
  var signature = calculateOdpsSignature_(stringToSign, p.accessKeySecret);
  headers['Authorization'] = 'ODPS ' + p.accessKeyId + ':' + signature;

  // 构造 URL（host 可能已包含 https://，需要处理）
  var host = p.host;
  if (host.indexOf('https://') === 0) {
    host = host.substring(8); // 去掉 "https://"
  } else if (host.indexOf('http://') === 0) {
    host = host.substring(7); // 去掉 "http://"
  }
  var url = 'https://' + host + resource;
  Logger.log('[odpsFetch] ' + method + ' host=' + host + ' resource=' + getOdpsResourceLogType_(p.pathname));

  // 发送请求
  var options = {
    method: method.toLowerCase(),
    headers: headers,
    muteHttpExceptions: true,
    validateHttpsCertificates: true
  };

  if (p.body && (method === 'POST' || method === 'PUT')) {
    options.payload = p.body;
  }

  var response = UrlFetchApp.fetch(url, options);
  Logger.log('[odpsFetch] HTTP ' + response.getResponseCode() + getOdpsRequestIdLogSuffix_(response));
  
  return response;
}

/**
 * 生成安全的资源日志类型，不记录 project/schema/table/instance 等业务标识。
 *
 * @param {string} pathname
 * @return {string}
 */
function getOdpsResourceLogType_(pathname) {
  pathname = String(pathname || '');
  if (/\/instances\/[^/]+$/.test(pathname)) {
    return 'instance';
  }
  if (/\/instances$/.test(pathname)) {
    return 'instances';
  }
  if (/\/schemas$/.test(pathname)) {
    return 'schemas';
  }
  if (/\/tables\/[^/]+$/.test(pathname)) {
    return 'table';
  }
  if (/\/tables$/.test(pathname)) {
    return 'tables';
  }
  return 'resource';
}


// ============================================================
// 签名计算
// ============================================================

/**
 * 构建 StringToSign
 *
 * StringToSign = HTTPMethod + "
"
 *              + Content-Type + "
"
 *              + Content-MD5 + "
"
 *              + Date + "
"
 *              + CanonicalizedODPSHeaders
 *              + CanonicalizedResource
 *
 * @param {string} method       - HTTP 方法
 * @param {Object} headers      - 请求头
 * @param {string} resource     - CanonicalizedResource
 * @param {string} prefix       - 自定义 header 前缀，默认 'x-odps-'
 * @return {string}
 */
function buildCanonicalString_(method, headers, resource, prefix) {
  var sb = [];
  prefix = prefix || 'x-odps-';

  // 1. HTTP Method
  sb.push(method.toUpperCase());
  sb.push('
');

  // 2. Content-MD5
  sb.push(getHeaderValue_(headers, 'Content-MD5') || '');
  sb.push('
');

  // 3. Content-Type
  sb.push(getHeaderValue_(headers, 'Content-Type') || '');
  sb.push('
');

  // 4. Date (必须)
  sb.push(getHeaderValue_(headers, 'Date') || '');
  sb.push('
');

  // 5. x-odps-* headers (按字母序排序)
  var odpsHeaders = [];
  for (var key in headers) {
    var lowerKey = key.toLowerCase();
    if (lowerKey.indexOf(prefix) === 0) {
      odpsHeaders.push(lowerKey);
    }
  }
  odpsHeaders.sort();

  for (var i = 0; i < odpsHeaders.length; i++) {
    var k = odpsHeaders[i];
    sb.push(k);
    sb.push(':');
    sb.push(getHeaderValue_(headers, k) || '');
    sb.push('
');
  }

  // 6. CanonicalizedResource
  sb.push(resource);

  return sb.join('');
}

/**
 * 构建 CanonicalizedResource
 *
 * 格式：/projects/{project}/instances?key1=value1&key2=value2
 * 参数按 key 字母序排序
 *
 * @param {string} pathname - URL 路径
 * @param {Object} params   - 查询参数 { key: value }
 * @return {string}
 */
function buildCanonicalResource_(pathname, params) {
  var encodedPathname = encodeCanonicalPath_(pathname);
  if (!params || Object.keys(params).length === 0) {
    return encodedPathname;
  }

  var keys = Object.keys(params).sort();
  var parts = [];
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    var v = params[k];
    if (v !== null && v !== undefined && v !== '') {
      parts.push(encodeCanonicalQueryPart_(k) + '=' + encodeCanonicalQueryPart_(v));
    } else {
      // 空值参数只写 key，如 ?result
      parts.push(encodeCanonicalQueryPart_(k));
    }
  }

  return encodedPathname + '?' + parts.join('&');
}

/**
 * 计算 ODPS V1 签名
 *
 * Signature = Base64(HMAC-SHA1(AccessKeySecret, StringToSign))
 *
 * @param {string} stringToSign - 待签名字符串
 * @param {string} accessKeySecret - AccessKey Secret
 * @return {string} Base64 编码的签名
 */
function calculateOdpsSignature_(stringToSign, accessKeySecret) {
  var bytes = Utilities.computeHmacSignature(
    Utilities.MacAlgorithm.HMAC_SHA_1,
    stringToSign,
    accessKeySecret
  );
  return Utilities.base64Encode(bytes);
}


// ============================================================
// 工具函数
// ============================================================

/**
 * 从 headers 对象中获取指定 key 的值（忽略大小写）
 *
 * @param {Object} headers - headers 对象
 * @param {string} key - 要查找的 key（不区分大小写）
 * @return {string|null}
 */
function getHeaderValue_(headers, key) {
  var lowerKey = key.toLowerCase();
  for (var k in headers) {
    if (k.toLowerCase() === lowerKey) {
      return headers[k];
    }
  }
  return null;
}

/**
 * 从 MaxCompute 响应 Header 中提取 request id，用于排查服务端错误。
 *
 * Header 名大小写不敏感，但只读取 MaxCompute 返回的 x-odps-request-id。
 *
 * @param {HTTPResponse} response
 * @return {string}
 */
function getOdpsResponseRequestId_(response) {
  if (!response) {
    return '';
  }

  var headers = {};
  try {
    if (typeof response.getAllHeaders === 'function') {
      headers = response.getAllHeaders() || {};
    }
  } catch (e) {}

  if (!headers || Object.keys(headers).length === 0) {
    try {
      if (typeof response.getHeaders === 'function') {
        headers = response.getHeaders() || {};
      }
    } catch (e2) {}
  }

  return normalizeOdpsRequestId_(getHeaderValue_(headers, 'x-odps-request-id'));
}

/**
 * 生成日志/错误信息中的 request id 后缀。
 *
 * @param {HTTPResponse} response
 * @return {string}
 */
function getOdpsRequestIdLogSuffix_(response) {
  var requestId = getOdpsResponseRequestId_(response);
  return requestId ? ' requestId=' + requestId : '';
}

/**
 * 清理 request id，避免异常 Header 值污染日志格式。
 *
 * @param {*} value
 * @return {string}
 */
function normalizeOdpsRequestId_(value) {
  if (value === null || value === undefined) {
    return '';
  }
  if (Array.isArray(value)) {
    value = value.length ? value[0] : '';
  }
  value = String(value || '').trim();
  if (!value) {
    return '';
  }
  value = value.replace(/[^A-Za-z0-9_.:-]+/g, '_');
  return value.substring(0, 128);
}

/**
 * 复制 query 参数，避免 odpsFetch_ 给调用方对象追加 curr_project 副作用。
 */
function copyQueryParams_(query) {
  var copy = {};
  query = query || {};
  for (var key in query) {
    if (Object.prototype.hasOwnProperty.call(query, key)) {
      copy[key] = query[key];
    }
  }
  return copy;
}

/**
 * 构造 ODPS path，并把每个 path segment 独立编码。
 *
 * 调用方不要用字符串拼接动态 project/table/instance 值；否则值里出现
 * "/" 时会被当成路径分隔符。这里先做 segment 编码，后续
 * buildCanonicalResource_ 再处理时也不会重复编码。
 *
 * @param {string[]} segments
 * @return {string}
 */
function buildOdpsPath_(segments) {
  segments = segments || [];
  var parts = [];
  for (var i = 0; i < segments.length; i++) {
    parts.push(encodeCanonicalPathPart_(segments[i]));
  }
  return '/' + parts.join('/');
}

/**
 * 编码 URL path，保留 "/" 分隔符。
 *
 * @param {string} pathname
 * @return {string}
 */
function encodeCanonicalPath_(pathname) {
  pathname = pathname || '/';
  var leadingSlash = pathname.charAt(0) === '/';
  var trailingSlash = pathname.length > 1 && pathname.charAt(pathname.length - 1) === '/';
  var parts = pathname.split('/');

  for (var i = 0; i < parts.length; i++) {
    parts[i] = encodeCanonicalPathPart_(parts[i]);
  }

  var encoded = parts.join('/');
  if (leadingSlash && encoded.charAt(0) !== '/') {
    encoded = '/' + encoded;
  }
  if (trailingSlash && encoded.charAt(encoded.length - 1) !== '/') {
    encoded += '/';
  }
  return encoded;
}

/**
 * 编码 path segment。
 */
function encodeCanonicalPathPart_(value) {
  value = String(value || '');
  try {
    return encodeURIComponent(decodeURIComponent(value));
  } catch (e) {
    return encodeURIComponent(value);
  }
}

/**
 * 编码 query key/value。
 */
function encodeCanonicalQueryPart_(value) {
  value = String(value || '');
  try {
    return encodeURIComponent(decodeURIComponent(value));
  } catch (e) {
    return encodeURIComponent(value);
  }
}
