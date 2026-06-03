// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * OSS Signature V1 签名实现
 *
 * 用于阿里云对象存储 (OSS) PutObject 等 API 的签名认证。
 * 签名格式：Authorization: OSS <AccessKeyId>:<Signature>
 *
 * 参考文档：
 * - 在 Header 中包含签名: https://help.aliyun.com/zh/oss/developer-reference/include-signatures-in-the-authorization-header
 */

// ============================================================
// 公开接口
// ============================================================

/**
 * OSS V1 签名请求
 *
 * @param {Object} p
 * @param {string} p.method          - HTTP 方法 GET/PUT/HEAD/DELETE
 * @param {string} p.region          - OSS 区域，如 oss-cn-hangzhou
 * @param {string} p.bucket          - Bucket 名称
 * @param {string} p.objectKey       - Object Key（路径）
 * @param {string} p.accessKeyId     - AccessKey ID
 * @param {string} p.accessKeySecret - AccessKey Secret
 * @param {string} [p.securityToken] - STS Token（可选）
 * @param {string|byte[]} [p.body]   - 请求体
 * @param {string} [p.contentType]   - Content-Type，默认 application/octet-stream
 * @param {Object} [p.extraHeaders]  - 额外的 x-oss-* 请求头
 * @return {HTTPResponse}
 */
function ossFetch_(p) {
  var method = (p.method || 'GET').toUpperCase();
  var date = Utilities.formatDate(new Date(), 'GMT', "EEE, dd MMM yyyy HH:mm:ss 'GMT'");
  var hasBody = !!(p.body && (method === 'PUT' || method === 'POST'));

  // OSS 协议：只有有 body 时才设置 Content-Type / Content-MD5
  var contentType = hasBody ? (p.contentType || 'application/octet-stream') : '';
  var contentMd5 = '';
  if (hasBody) {
    var bodyBytes = (typeof p.body === 'string')
      ? Utilities.newBlob(p.body, 'text/plain; charset=utf-8').getBytes()
      : p.body;
    var md5Bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, bodyBytes);
    contentMd5 = Utilities.base64Encode(md5Bytes);
  }

  // 构造请求头（仅包含实际要发送的 header）
  var headers = {
    'Date': date,
    'User-Agent': MC_GSHEET_PLUGIN_USER_AGENT
  };

  // 有 body 时才加 Content-MD5 / Content-Type
  if (contentMd5) {
    headers['Content-MD5'] = contentMd5;
  }
  if (contentType) {
    headers['Content-Type'] = contentType;
  }

  // STS Token
  if (p.securityToken) {
    headers['x-oss-security-token'] = p.securityToken;
  }

  // 额外自定义 headers
  if (p.extraHeaders) {
    for (var key in p.extraHeaders) {
      if (Object.prototype.hasOwnProperty.call(p.extraHeaders, key)) {
        headers[key.toLowerCase()] = p.extraHeaders[key];
      }
    }
  }

  // 构建 CanonicalizedResource
  var resource = buildOssCanonicalResource_(p.bucket, p.objectKey);

  // 构建 StringToSign（签名中 Content-MD5/Content-Type 使用实际值或空字符串）
  var stringToSign = buildOssStringToSign_(method, contentMd5, contentType, date, headers, resource);

  // 计算签名
  var signature = calculateOssSignature_(stringToSign, p.accessKeySecret);
  headers['Authorization'] = 'OSS ' + p.accessKeyId + ':' + signature;

  // 构造 URL（virtual-hosted style：{bucket}.{region}.aliyuncs.com/{objectKey}）
  // OSS 新建 bucket 默认禁用 path-style，path-style 会返回 SecondLevelDomainForbidden。
  var url = 'https://' + p.bucket + '.' + p.region + '.aliyuncs.com/' + encodeOssObjectKey_(p.objectKey);

  Logger.log('[ossFetch] ' + method + ' region=' + p.region + ' bucket=' + p.bucket +
    ' objectKey=' + truncateLogValue_(p.objectKey, 80));

  // 发送请求
  var options = {
    method: method.toLowerCase(),
    headers: headers,
    muteHttpExceptions: true,
    validateHttpsCertificates: true
  };

  if (hasBody) {
    options.payload = (typeof p.body === 'string')
      ? Utilities.newBlob(p.body, contentType).getBytes()
      : p.body;
    options.contentType = contentType;
    // Content-Type 已通过 options.contentType 传递，从 headers 中删除避免重复
    delete options.headers['Content-Type'];
  }

  var response = UrlFetchApp.fetch(url, options);
  Logger.log('[ossFetch] HTTP ' + response.getResponseCode());

  return response;
}


// ============================================================
// 签名计算
// ============================================================

/**
 * 构建 OSS V1 StringToSign
 *
 * StringToSign = VERB + "
"
 *              + Content-MD5 + "
"
 *              + Content-Type + "
"
 *              + Date + "
"
 *              + CanonicalizedOSSHeaders
 *              + CanonicalizedResource
 *
 * 注意与 ODPS 的区别：
 * - OSS 使用 x-oss-* 头（ODPS 使用 x-odps-*）
 * - OSS CanonicalizedResource = /{Bucket}/{ObjectKey}?sub-resource（不含普通 query 参数）
 * - 无 body 时 Content-MD5 和 Content-Type 为空字符串（不在请求头中发送）
 *
 * @param {string} method      - HTTP 方法
 * @param {string} contentMd5  - Content-MD5 值（无 body 时为空字符串）
 * @param {string} contentType - Content-Type 值（无 body 时为空字符串）
 * @param {string} date        - RFC 2616 格式日期
 * @param {Object} headers     - 请求头（用于提取 x-oss-* headers）
 * @param {string} resource    - CanonicalizedResource
 * @return {string}
 */
function buildOssStringToSign_(method, contentMd5, contentType, date, headers, resource) {
  var sb = [];

  // 1. HTTP Method
  sb.push(method.toUpperCase());
  sb.push('
');

  // 2. Content-MD5（无 body 时为空）
  sb.push(contentMd5);
  sb.push('
');

  // 3. Content-Type（无 body 时为空）
  sb.push(contentType);
  sb.push('
');

  // 4. Date
  sb.push(date);
  sb.push('
');

  // 5. CanonicalizedOSSHeaders（x-oss-* 按字母序）
  sb.push(buildOssCanonicalizedHeaders_(headers));

  // 6. CanonicalizedResource
  sb.push(resource);

  return sb.join('');
}

/**
 * 构建 CanonicalizedOSSHeaders
 *
 * 将所有以 x-oss- 开头的请求头按 key 字母序排列，
 * 格式为 "key:value
"
 *
 * @param {Object} headers
 * @return {string}
 */
function buildOssCanonicalizedHeaders_(headers) {
  var ossHeaders = [];
  for (var key in headers) {
    if (Object.prototype.hasOwnProperty.call(headers, key)) {
      var lowerKey = key.toLowerCase();
      if (lowerKey.indexOf('x-oss-') === 0) {
        ossHeaders.push(lowerKey + ':' + String(headers[key]).trim());
      }
    }
  }

  if (ossHeaders.length === 0) {
    return '';
  }

  ossHeaders.sort();
  return ossHeaders.join('
') + '
';
}

/**
 * 构建 CanonicalizedResource
 *
 * 格式：/{bucket}/{objectKey}
 * 对于 PutObject 等操作，不含 subresource 参数。
 *
 * @param {string} bucket    - Bucket 名称
 * @param {string} objectKey - Object Key
 * @return {string}
 */
function buildOssCanonicalResource_(bucket, objectKey) {
  var resource = '/' + bucket + '/';
  if (objectKey) {
    resource += objectKey;
  }
  return resource;
}

/**
 * 计算 OSS V1 签名
 *
 * Signature = Base64(HMAC-SHA1(AccessKeySecret, StringToSign))
 *
 * @param {string} stringToSign    - 待签名字符串
 * @param {string} accessKeySecret - AccessKey Secret
 * @return {string} Base64 编码的签名
 */
function calculateOssSignature_(stringToSign, accessKeySecret) {
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
 * 编码 OSS Object Key（path-style URL 中 bucket/objectKey 部分）。
 *
 * 保留 "/" 分隔符不编码。
 *
 * @param {string} path - bucket/objectKey
 * @return {string}
 */
function encodeOssObjectKey_(path) {
  if (!path) return '';
  var segments = path.split('/');
  for (var i = 0; i < segments.length; i++) {
    segments[i] = encodeURIComponent(segments[i]);
  }
  return segments.join('/');
}

/**
 * 截断日志值，避免过长的 objectKey 污染日志。
 *
 * @param {string} value
 * @param {number} maxLen
 * @return {string}
 */
function truncateLogValue_(value, maxLen) {
  value = String(value || '');
  if (value.length <= maxLen) return value;
  return value.substring(0, maxLen) + '...';
}
