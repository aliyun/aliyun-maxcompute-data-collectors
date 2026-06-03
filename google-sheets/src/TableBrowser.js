// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * 表浏览器 API
 *
 * 提供 MaxCompute Schema、表列表、表结构、分区信息的查询功能。
 * 支持三层结构：Project → Schema → Tables
 *
 * API 设计：
 * - Schema 通过 query 参数 curr_schema 指定（而非 URL 路径）
 * - Project 通过 query 参数 curr_project 指定
 */

// ============================================================
// 内部接口
// ============================================================

/**
 * 获取 Schema 列表
 *
 * @return {Object[]} Schema 列表 [{ name, owner, creationTime }]
 */
function listSchemas_() {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'schemas']),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: { maxitems: '1000' }
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throwCatalogHttpError_('获取 Schema 列表失败', code, response);
  }

  return parseSchemasXmlStrict_(response.getContentText());
}

/**
 * 获取指定 Schema 下的表列表
 *
 * @param {string} schemaName - Schema 名称（可选，默认为 default）
 * @param {string} prefix - 表名前缀过滤（可选）
 * @return {Object[]} 表列表 [{ name, type, comment }]
 */
function listTables_(schemaName, prefix) {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  // 构建 query 参数
  var query = { maxitems: '1000' };

  // Schema 通过 curr_schema 参数指定
  if (schemaName) {
    query['curr_schema'] = schemaName;
  }

  if (prefix) {
    query['prefix'] = prefix;
  }

  // URL 路径不带 schema
  var pathname = buildOdpsPath_(['projects', config.project, 'tables']);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: pathname,
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: query
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throwCatalogHttpError_('获取表列表失败', code, response);
  }

  return parseTablesXmlStrict_(response.getContentText());
}

/**
 * 获取表结构（包含字段信息）
 *
 * @param {string} tableName - 表名
 * @param {string} schemaName - Schema 名称（可选，默认为 default）
 * @return {Object} { name, type, comment, columns, partitionColumns }
 */
function getTableSchema_(tableName, schemaName) {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  // 构建 query 参数
  var query = { asynccache: '' };

  // Schema 通过 curr_schema 参数指定
  if (schemaName) {
    query['curr_schema'] = schemaName;
  }

  // URL 路径不带 schema
  var pathname = buildOdpsPath_(['projects', config.project, 'tables', tableName]);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: pathname,
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: query
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throwCatalogHttpError_('获取表结构失败', code, response);
  }

  return parseTableSchemaXmlStrict_(response.getContentText());
}

/**
 * 获取分区列表
 *
 * 参考 Java SDK: Table.getPartitionSpecs()
 * API: GET /projects/{project}/tables/{table}?partitions&name&curr_schema={schema}
 *
 * @param {string} tableName - 表名
 * @param {string} schemaName - Schema 名称（可选）
 * @return {Object[]} 分区列表 [{ name, value, creationTime, size }]
 */
function listPartitions_(tableName, schemaName) {
  var config = getMcConfig_();
  assertUsableMcConfig_(config);

  // 参考 Java SDK: params.put("partitions", null); params.put("name", null);
  var query = {
    'partitions': '',
    'name': ''
  };

  // Schema 通过 curr_schema 参数指定
  if (schemaName) {
    query['curr_schema'] = schemaName;
  }

  // URL 路径: /projects/{project}/tables/{table}
  var pathname = buildOdpsPath_(['projects', config.project, 'tables', tableName]);

  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: pathname,
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: query
  });

  var code = response.getResponseCode();
  if (code !== 200) {
    throwCatalogHttpError_('获取分区列表失败', code, response);
  }

  return parsePartitionSpecsXmlStrict_(response.getContentText());
}

function throwCatalogHttpError_(prefix, code, response) {
  var message = parseErrorSummary_(response.getContentText() || '');
  throw new Error(prefix + ' (HTTP ' + code + ')' + getOdpsRequestIdLogSuffix_(response) + ': ' + message);
}

function throwCatalogParseError_(prefix, e) {
  throw new Error(prefix + ': ' + String(e && e.message ? e.message : e || ''));
}

// ============================================================
// XML 解析
// ============================================================

/**
 * 解析 Schema 列表 XML
 */
function parseSchemasXml_(xml) {
  var schemas = [];

  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();

    // Schemas/Schema
    var schemaElements = root.getChildren('Schema');
    for (var i = 0; i < schemaElements.length; i++) {
      var elem = schemaElements[i];
      schemas.push({
        name: getChildText_(elem, 'Name'),
        owner: getChildText_(elem, 'Owner') || '',
        creationTime: getChildText_(elem, 'CreationTime') || ''
      });
    }
  } catch (e) {
    Logger.log('[TableBrowser] 解析 Schema 列表 XML 失败: ' + (e && e.message ? e.message : e));
  }

  return schemas;
}

function parseSchemasXmlStrict_(xml) {
  try {
    return parseSchemasXmlUnsafe_(xml);
  } catch (e) {
    throwCatalogParseError_('解析 Schema 列表 XML 失败', e);
  }
}

function parseSchemasXmlUnsafe_(xml) {
  var schemas = [];
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();
  assertXmlRootName_(root, ['Schemas']);

  var schemaElements = root.getChildren('Schema');
  for (var i = 0; i < schemaElements.length; i++) {
    var elem = schemaElements[i];
    schemas.push({
      name: getChildText_(elem, 'Name'),
      owner: getChildText_(elem, 'Owner') || '',
      creationTime: getChildText_(elem, 'CreationTime') || ''
    });
  }
  return schemas;
}

/**
 * 解析表列表 XML
 */
function parseTablesXml_(xml) {
  var tables = [];

  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();

    // Tables/Table
    var tableElements = root.getChildren('Table');
    for (var i = 0; i < tableElements.length; i++) {
      var elem = tableElements[i];
      tables.push({
        name: getChildText_(elem, 'Name'),
        type: getChildText_(elem, 'Type') || 'MANAGED_TABLE',
        comment: getChildText_(elem, 'Comment') || ''
      });
    }
  } catch (e) {
    Logger.log('[TableBrowser] 解析表列表 XML 失败: ' + (e && e.message ? e.message : e));
  }

  return tables;
}

function parseTablesXmlStrict_(xml) {
  try {
    return parseTablesXmlUnsafe_(xml);
  } catch (e) {
    throwCatalogParseError_('解析表列表 XML 失败', e);
  }
}

function parseTablesXmlUnsafe_(xml) {
  var tables = [];
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();
  assertXmlRootName_(root, ['Tables']);

  var tableElements = root.getChildren('Table');
  for (var i = 0; i < tableElements.length; i++) {
    var elem = tableElements[i];
    tables.push({
      name: getChildText_(elem, 'Name'),
      type: getChildText_(elem, 'Type') || 'MANAGED_TABLE',
      comment: getChildText_(elem, 'Comment') || ''
    });
  }
  return tables;
}

/**
 * 解析表结构 XML
 *
 * Schema 可能是两种格式：
 * 1. JSON 格式：<Schema format="Json">{...}</Schema>
 * 2. XML 格式：<Schema><Column>...</Column></Schema>
 */
function parseTableSchemaXml_(xml) {
  var result = {
    name: '',
    type: '',
    comment: '',
    columns: [],
    partitionColumns: []
  };

  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();

    result.name = getChildText_(root, 'Name');
    result.type = getChildText_(root, 'Type') || 'MANAGED_TABLE';
    result.comment = getChildText_(root, 'Comment') || '';

    // 解析 Schema 元素
    var schemaElem = root.getChild('Schema');
    if (schemaElem) {
      var format = schemaElem.getAttribute('format');
      var schemaText = schemaElem.getText();

      if (format && format.getValue().toLowerCase() === 'json' && schemaText) {
        // JSON 格式
        parseTableSchemaJson_(schemaText, result);
      } else {
        // XML 格式
        var columnElems = schemaElem.getChildren('Column');
        for (var i = 0; i < columnElems.length; i++) {
          var col = columnElems[i];
          var column = {
            name: getChildText_(col, 'Name'),
            type: getChildText_(col, 'Type'),
            comment: getChildText_(col, 'Comment') || '',
            nullable: getChildText_(col, 'Nullable') !== 'false'
          };
          result.columns.push(column);
        }
      }
    }

    // 解析 PartitionKeys/Column（XML 格式）
    var partitionKeysElem = root.getChild('PartitionKeys');
    if (partitionKeysElem) {
      var partitionColumnElems = partitionKeysElem.getChildren('Column');
      for (var j = 0; j < partitionColumnElems.length; j++) {
        var pcol = partitionColumnElems[j];
        var partitionColumn = {
          name: getChildText_(pcol, 'Name'),
          type: getChildText_(pcol, 'Type'),
          comment: getChildText_(pcol, 'Comment') || ''
        };
        result.partitionColumns.push(partitionColumn);
      }
    }
  } catch (e) {
    Logger.log('[TableBrowser] 解析表结构 XML 失败: ' + (e && e.message ? e.message : e));
  }

  return result;
}

function parseTableSchemaXmlStrict_(xml) {
  try {
    return parseTableSchemaXmlUnsafe_(xml);
  } catch (e) {
    throwCatalogParseError_('解析表结构 XML 失败', e);
  }
}

function parseTableSchemaXmlUnsafe_(xml) {
  var result = {
    name: '',
    type: '',
    comment: '',
    columns: [],
    partitionColumns: []
  };
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();
  assertXmlRootName_(root, ['Table']);

  result.name = getChildText_(root, 'Name');
  result.type = getChildText_(root, 'Type') || 'MANAGED_TABLE';
  result.comment = getChildText_(root, 'Comment') || '';

  var schemaElem = root.getChild('Schema');
  if (schemaElem) {
    var format = schemaElem.getAttribute('format');
    var schemaText = schemaElem.getText();

    if (format && format.getValue().toLowerCase() === 'json' && schemaText) {
      parseTableSchemaJsonStrict_(schemaText, result);
    } else {
      var columnElems = schemaElem.getChildren('Column');
      for (var i = 0; i < columnElems.length; i++) {
        var col = columnElems[i];
        result.columns.push({
          name: getChildText_(col, 'Name'),
          type: getChildText_(col, 'Type'),
          comment: getChildText_(col, 'Comment') || '',
          nullable: getChildText_(col, 'Nullable') !== 'false'
        });
      }
    }
  }

  var partitionKeysElem = root.getChild('PartitionKeys');
  if (partitionKeysElem) {
    var partitionColumnElems = partitionKeysElem.getChildren('Column');
    for (var j = 0; j < partitionColumnElems.length; j++) {
      var pcol = partitionColumnElems[j];
      result.partitionColumns.push({
        name: getChildText_(pcol, 'Name'),
        type: getChildText_(pcol, 'Type'),
        comment: getChildText_(pcol, 'Comment') || ''
      });
    }
  }
  return result;
}

/**
 * 解析 JSON 格式的 Schema
 *
 * @param {string} jsonText - JSON 文本
 * @param {Object} result - 结果对象（会被修改）
 */
function parseTableSchemaJson_(jsonText, result) {
  try {
    var schema = JSON.parse(jsonText);

    // 解析普通列
    if (schema.columns && Array.isArray(schema.columns)) {
      schema.columns.forEach(function(col) {
        result.columns.push({
          name: col.name || '',
          type: col.type || '',
          comment: col.comment || '',
          nullable: col.isNullable !== false
        });
      });
    }

    // 解析分区列
    if (schema.partitionKeys && Array.isArray(schema.partitionKeys)) {
      schema.partitionKeys.forEach(function(col) {
        result.partitionColumns.push({
          name: col.name || '',
          type: col.type || '',
          comment: col.comment || ''
        });
      });
    }
  } catch (e) {
    Logger.log('[TableBrowser] 解析 JSON Schema 失败: ' + (e && e.message ? e.message : e));
  }
}

function parseTableSchemaJsonStrict_(jsonText, result) {
  var schema = JSON.parse(jsonText);
  var hasColumns = schema && Array.isArray(schema.columns);
  var hasPartitionKeys = schema && Array.isArray(schema.partitionKeys);
  if (!hasColumns && !hasPartitionKeys) {
    throw new Error('Unexpected JSON schema shape');
  }

  if (hasColumns) {
    schema.columns.forEach(function(col) {
      result.columns.push({
        name: col.name || '',
        type: col.type || '',
        comment: col.comment || '',
        nullable: col.isNullable !== false
      });
    });
  }

  if (hasPartitionKeys) {
    schema.partitionKeys.forEach(function(col) {
      result.partitionColumns.push({
        name: col.name || '',
        type: col.type || '',
        comment: col.comment || ''
      });
    });
  }
}

/**
 * 解析分区列表 XML
 *
 * 响应格式：
 * <Table>
 *   <Partition><Name>pt=20240101</Name></Partition>
 *   <Partition><Name>pt=20240102</Name></Partition>
 * </Table>
 */
function parsePartitionSpecsXml_(xml) {
  var partitions = [];

  try {
    var doc = XmlService.parse(xml);
    var root = doc.getRootElement();

    // Partition 元素直接在根元素下
    var partitionElems = root.getChildren('Partition');
    for (var i = 0; i < partitionElems.length; i++) {
      var elem = partitionElems[i];
      var name = getChildText_(elem, 'Name');
      partitions.push({
        name: name,
        value: name,
        creationTime: '',
        size: ''
      });
    }
  } catch (e) {
    Logger.log('[TableBrowser] 解析分区列表 XML 失败: ' + (e && e.message ? e.message : e));
  }

  return partitions;
}

function parsePartitionSpecsXmlStrict_(xml) {
  try {
    return parsePartitionSpecsXmlUnsafe_(xml);
  } catch (e) {
    throwCatalogParseError_('解析分区列表 XML 失败', e);
  }
}

function parsePartitionSpecsXmlUnsafe_(xml) {
  var partitions = [];
  var doc = XmlService.parse(xml);
  var root = doc.getRootElement();
  assertXmlRootName_(root, ['Table']);

  var partitionElems = root.getChildren('Partition');
  for (var i = 0; i < partitionElems.length; i++) {
    var elem = partitionElems[i];
    var name = getChildText_(elem, 'Name');
    partitions.push({
      name: name,
      value: name,
      creationTime: '',
      size: ''
    });
  }
  return partitions;
}

function assertXmlRootName_(root, expectedNames) {
  var rootName = root && root.getName ? String(root.getName()) : '';
  for (var i = 0; i < expectedNames.length; i++) {
    if (rootName === expectedNames[i]) {
      return;
    }
  }
  throw new Error('Unexpected XML root: expected=' + expectedNames.join('|') +
    ' actual=' + rootName);
}
