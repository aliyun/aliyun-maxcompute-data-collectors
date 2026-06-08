// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * 测试用例 — 在 Apps Script 编辑器中直接运行
 * 
 * 运行前先在「MaxCompute → 设置连接」中保存 AK/SK/Project。
 * 配置保存在用户属性中；旧版脚本属性配置仍可作为读取兜底。
 */


// ============================================================
// 数据目录测试
// ============================================================

function qaErrorMessage_(e) {
  return e && e.message ? String(e.message) : String(e || '');
}

/**
 * 测试 1：列出 Schema
 */
function test_listSchemas() {
  Logger.log('=== 测试列出 Schema ===');

  try {
    var schemas = listSchemas_();
    Logger.log('✅ 成功获取 Schema 列表');
    Logger.log('Schema 数量: ' + schemas.length);
    Logger.log('前 10 个 Schema 名称: ' + schemas.slice(0, 10).map(function(schema) {
      return schema.name;
    }).join(','));
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 2：列出指定 Schema 下的表
 */
function test_listTables() {
  Logger.log('=== 测试列出表 ===');

  try {
    // 先获取 Schema 列表
    var schemas = listSchemas_();
    if (schemas.length === 0) {
      Logger.log('❌ 没有 Schema');
      return;
    }

    var schemaName = schemas[0].name;
    Logger.log('使用 Schema: ' + schemaName);

    var tables = listTables_(schemaName);
    Logger.log('✅ 成功获取表列表');
    Logger.log('表数量: ' + tables.length);

    Logger.log('前 10 个表名: ' + tables.slice(0, 10).map(function(table) {
      return table.name;
    }).join(','));

    if (tables.length > 10) {
      Logger.log('  ... 还有 ' + (tables.length - 10) + ' 个表');
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 3：获取表结构
 */
function test_getTableSchema() {
  Logger.log('=== 测试获取表结构 ===');

  try {
    // 先获取一个表
    var schemas = listSchemas_();
    if (schemas.length === 0) {
      Logger.log('❌ 没有 Schema');
      return;
    }

    var tables = listTables_(schemas[0].name);
    if (tables.length === 0) {
      Logger.log('❌ 没有表');
      return;
    }

    var tableName = tables[0].name;
    var schemaName = schemas[0].name;
    Logger.log('获取表: schema=' + schemaName + ', table=' + tableName);

    var schema = getTableSchema_(tableName, schemaName);
    Logger.log('✅ 成功获取表结构');
    Logger.log('表名: ' + schema.name);
    Logger.log('类型: ' + schema.type);
    Logger.log('注释: ' + schema.comment);

    Logger.log('\n普通列 (' + schema.columns.length + '):');
    Logger.log('前 20 个普通列名: ' + schema.columns.slice(0, 20).map(function(col) {
      return col.name;
    }).join(','));

    if (schema.partitionColumns && schema.partitionColumns.length > 0) {
      Logger.log('\n分区列数量: ' + schema.partitionColumns.length);
      Logger.log('前 20 个分区列名: ' + schema.partitionColumns.slice(0, 20).map(function(col) {
        return col.name;
      }).join(','));
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 4：获取分区列表
 */
function test_listPartitions() {
  Logger.log('=== 测试获取分区列表 ===');

  try {
    // 找一个分区表
    var schemas = listSchemas_();
    var found = false;

    for (var i = 0; i < schemas.length && !found; i++) {
      var tables = listTables_(schemas[i].name);
      for (var j = 0; j < tables.length; j++) {
        var schema = getTableSchema_(tables[j].name, schemas[i].name);
        if (schema.partitionColumns && schema.partitionColumns.length > 0) {
          Logger.log('找到分区表: schema=' + schemas[i].name + ', table=' + tables[j].name);

          var partitions = listPartitions_(tables[j].name, schemas[i].name);
          Logger.log('✅ 成功获取分区列表');
          Logger.log('分区数量: ' + partitions.length);
          Logger.log('前 10 个分区名: ' + partitions.slice(0, 10).map(function(p) {
            return p.name;
          }).join(','));

          if (partitions.length > 10) {
            Logger.log('  ... 还有 ' + (partitions.length - 10) + ' 个分区');
          }

          found = true;
          break;
        }
      }
    }

    if (!found) {
      Logger.log('⚠️ 没有找到分区表');
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 5：获取表详情（包含字段和分区）
 */
function test_getTableDetail() {
  Logger.log('=== 测试获取表详情 ===');

  try {
    var schemas = listSchemas_();
    if (schemas.length === 0) {
      Logger.log('❌ 没有 Schema');
      return;
    }

    var tables = listTables_(schemas[0].name);
    if (tables.length === 0) {
      Logger.log('❌ 没有表');
      return;
    }

    var detail = getTableDetail(tables[0].name, schemas[0].name);
    Logger.log('✅ 成功获取表详情');
    Logger.log('表名: ' + detail.name);
    Logger.log('Schema 名称: ' + detail.schemaName);
    Logger.log('类型: ' + detail.type);
    Logger.log('列数: ' + detail.columns.length);

    if (detail.partitionColumns && detail.partitionColumns.length > 0) {
      Logger.log('分区列名: ' + detail.partitionColumns.map(function(c) { return c.name; }).join(','));
      Logger.log('分区数: ' + (detail.partitions ? detail.partitions.length : 0));
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}


// ============================================================
// SQL 执行测试
// ============================================================

/**
 * 测试 6：执行简单 SQL 查询
 */
function test_executeSimpleQuery() {
  Logger.log('=== 测试简单 SQL 查询 ===');

  var sql = 'SELECT 1 AS id, "hello" AS name;';

  try {
    var result = executeSqlQuery_(sql);
    Logger.log('✅ 执行成功');
    Logger.log('列数: ' + (result.columns ? result.columns.length : 0));
    Logger.log('行数: ' + result.rowCount);
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 7：执行带 LIMIT 的查询
 */
function test_executeQueryWithLimit() {
  Logger.log('=== 测试带 LIMIT 的 SQL 查询 ===');

  // 请替换为实际存在的表名
  var sql = 'SELECT * FROM dual LIMIT 5';

  try {
    var result = executeSqlQuery_(sql);
    Logger.log('✅ 执行成功');
    Logger.log('列数: ' + (result.columns ? result.columns.length : 0));
    Logger.log('行数: ' + result.rowCount);
    if (result.instanceId) {
      Logger.log('Instance ID: ' + result.instanceId);
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 8：测试错误 SQL
 */
function test_executeInvalidSql() {
  Logger.log('=== 测试错误 SQL ===');

  var sql = 'SELECT * FROM non_existent_table_12345';

  try {
    var result = executeSqlQuery_(sql);
    Logger.log('❌ 应该抛出错误但没有');
  } catch (e) {
    Logger.log('✅ 正确捕获错误: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 9：测试只读 SQL 防护（不依赖真实 MaxCompute）
 */
function test_readOnlySqlGuard() {
  Logger.log('=== 测试只读 SQL 防护 ===');

  try {
    assertReadOnlySql_('SET odps.sql.mapper.split.size=256; SELECT 1 AS id;');
    Logger.log('✅ SELECT / SET + SELECT 允许通过');
  } catch (e) {
    Logger.log('❌ 只读 SQL 被误拦截: ' + e.message);
    return;
  }

  var blockedSql = [
    'INSERT INTO t SELECT 1;',
    'UPDATE t SET id = 1;',
    'DELETE FROM t WHERE id = 1;',
    'CREATE TABLE t (id BIGINT);',
    'DROP TABLE t;',
    'GRANT SELECT ON TABLE t TO USER u;'
  ];

  blockedSql.forEach(function(sql, index) {
    try {
      assertReadOnlySql_(sql);
      Logger.log('❌ 应该被拦截但通过: case=' + (index + 1) + ' sql=' + sql);
    } catch (e) {
      Logger.log('✅ 正确拦截: case=' + (index + 1) + ' sql=' + sql + ' -> ' + e.message);
    }
  });
}

/**
 * 测试 10：测试 Endpoint 格式校验（不依赖真实 MaxCompute）
 */
function test_endpointValidation() {
  Logger.log('=== 测试 Endpoint 格式校验 ===');

  try {
    assertValidMcEndpoint_('https://service.ap-southeast-1.maxcompute.aliyun.com/api');
    Logger.log('✅ 合法 MaxCompute Endpoint 允许通过');
  } catch (e) {
    Logger.log('❌ 合法 Endpoint 被误拦截: ' + qaErrorMessage_(e));
  }

  ['http://service.ap-southeast-1.maxcompute.aliyun.com/api', 'https://example.com/api'].forEach(function(endpoint) {
    try {
      assertValidMcEndpoint_(endpoint);
      Logger.log('❌ 应该被拦截但通过: endpoint=' + endpoint);
    } catch (e) {
      Logger.log('✅ 正确拦截: endpoint=' + endpoint + ' -> ' + qaErrorMessage_(e));
    }
  });
}

/**
 * 测试 11：测试 ODPS 签名
 */
function test_odpsSignature() {
  Logger.log('=== 测试 ODPS 签名 ===');
  
  try {
    var config = getMcConfig_();
    
    // 测试一个简单的 GET 请求
    var response = odpsFetch_({
      method: 'GET',
      host: config.endpoint,
      pathname: buildOdpsPath_(['projects', config.project, 'instances']),
      accessKeyId: config.accessKeyId,
      accessKeySecret: config.accessKeySecret,
      securityToken: config.securityToken || null,
      project: config.project,
      query: { maxitems: '1' }
    });
    
    var code = response.getResponseCode();
    Logger.log('HTTP 状态码: ' + code);
    
    if (code === 200) {
      Logger.log('✅ 签名验证成功');
    } else if (code === 401 || code === 403) {
      Logger.log('❌ 签名验证失败（认证错误）');
      Logger.log('HTTP 状态码: ' + code);
    } else {
      Logger.log('HTTP 状态码: ' + code);
    }
  } catch (e) {
    Logger.log('❌ 请求失败: ' + qaErrorMessage_(e));
  }
}

/**
 * 测试 12：测试连接状态
 */
function test_connectionStatus() {
  Logger.log('=== 测试连接状态 ===');

  try {
    var status = getConnectionStatus();
    Logger.log('已配置: ' + status.configured);
    Logger.log('项目: ' + status.project);

    if (status.configured) {
      var testResult = testConnection();
      Logger.log('连接测试: ' + (testResult.success ? '✅ 成功' : '❌ 失败'));
      Logger.log('消息: ' + testResult.message);
    }
  } catch (e) {
    Logger.log('❌ 失败: ' + qaErrorMessage_(e));
  }
}


// ============================================================
// 发布前 Smoke Test（结构化 pass/fail）
// ============================================================

/**
 * 运行发布前 Smoke Test。
 *
 * 默认会调用真实 MaxCompute 服务；发布前请在 Apps Script 编辑器中直接运行
 * `runReleaseSmokeTests()`，并把返回摘要/日志填入 docs/external-qa-evidence-template.md。
 *
 * 本地或只验证安全规则时可运行：
 *   runReleaseSmokeTests({ includeRealService: false })
 *
 * @param {Object} options - { includeRealService?: boolean }
 * @return {Object} { passed, failed, skipped, results }
 * @throws {Error} 任一必需检查失败时抛错
 */
function runReleaseSmokeTests(options) {
  options = options || {};
  var includeRealService = options.includeRealService !== false;
  var steps = [
    { name: 'read_only_sql_guard', fn: qaCheckReadOnlySqlGuard_ },
    { name: 'endpoint_validation', fn: qaCheckEndpointValidation_ }
  ];

  if (includeRealService) {
    steps.push(
      { name: 'connection_status', fn: qaCheckConnection_ },
      { name: 'catalog_browse', fn: qaCheckCatalog_ },
      { name: 'simple_query', fn: qaCheckSimpleQuery_ },
      { name: 'odps_signature', fn: qaCheckOdpsSignature_ }
    );
  } else {
    steps.push({
      name: 'real_service_checks',
      fn: function() {
        return 'Skipped by includeRealService=false';
      },
      optional: true,
      skipped: true
    });
  }

  return runQaSteps_('Release Smoke Tests', steps);
}

/**
 * 只运行不依赖真实 MaxCompute 的本地安全 smoke test。
 */
function runLocalSafetySmokeTests() {
  return runReleaseSmokeTests({ includeRealService: false });
}

function runQaSteps_(title, steps) {
  Logger.log('========================================');
  Logger.log(title);
  Logger.log('========================================');

  var results = [];
  var failed = [];
  var skipped = 0;

  for (var i = 0; i < steps.length; i++) {
    var step = steps[i];
    var result;
    if (step.skipped) {
      skipped++;
      result = qaResult_(step.name, 'SKIPPED', step.fn());
    } else {
      try {
        result = qaResult_(step.name, 'PASSED', step.fn());
      } catch (e) {
        result = qaResult_(step.name, step.optional ? 'SKIPPED' : 'FAILED', e.message || String(e));
        if (step.optional) {
          skipped++;
        } else {
          failed.push(step.name + ': ' + (e.message || String(e)));
        }
      }
    }

    results.push(result);
    Logger.log(result.status + ' ' + result.name + ' - ' + result.message);

    if (failed.length > 0) {
      break;
    }
  }

  var summary = {
    passed: results.filter(function(r) { return r.status === 'PASSED'; }).length,
    failed: failed.length,
    skipped: skipped,
    results: results
  };

  Logger.log('========================================');
  Logger.log('Smoke summary: ' + JSON.stringify(summary));
  Logger.log('========================================');

  if (failed.length > 0) {
    throw new Error('Release smoke tests failed: ' + failed.join(' | '));
  }

  return summary;
}

function qaResult_(name, status, message) {
  return {
    name: name,
    status: status,
    message: message || ''
  };
}

function qaAssert_(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function qaCheckReadOnlySqlGuard_() {
  var allowed = [
    'SET odps.sql.mapper.split.size=256; SELECT 1 AS id;',
    'WITH c AS (SELECT 1 AS id) SELECT * FROM c;'
  ];

  allowed.forEach(function(sql) {
    assertReadOnlySql_(sql);
  });

  var blocked = [
    'INSERT INTO t SELECT 1;',
    'UPDATE t SET id = 1;',
    'DELETE FROM t WHERE id = 1;',
    'CREATE TABLE t (id BIGINT);',
    'DROP TABLE t;',
    'GRANT SELECT ON TABLE t TO USER u;',
    'SHOW TABLES;',
    'SHOW CREATE TABLE some_table;',
    'DESC some_table;',
    'DESCRIBE some_table;',
    'EXPLAIN SELECT * FROM some_table;',
    'EXPLAIN INSERT INTO t SELECT 1;'
  ];

  blocked.forEach(function(sql) {
    var blockedOk = false;
    try {
      assertReadOnlySql_(sql);
    } catch (e) {
      blockedOk = true;
    }
    qaAssert_(blockedOk, 'SQL should be blocked: ' + sql);
  });

  return allowed.length + ' allowed cases and ' + blocked.length + ' blocked cases verified';
}

function qaCheckEndpointValidation_() {
  assertValidMcEndpoint_('https://service.ap-southeast-1.maxcompute.aliyun.com/api');

  var invalid = [
    'http://service.ap-southeast-1.maxcompute.aliyun.com/api',
    'https://example.com/api',
    'https://maxcompute.console.aliyun.com/api'
  ];

  invalid.forEach(function(endpoint) {
    var blocked = false;
    try {
      assertValidMcEndpoint_(endpoint);
    } catch (e) {
      blocked = true;
    }
    qaAssert_(blocked, 'Endpoint should be blocked: ' + endpoint);
  });

  return 'Endpoint allow/deny cases verified';
}

function qaCheckConnection_() {
  var status = getConnectionStatus();
  qaAssert_(status.configured, 'Connection is not configured');

  var result = testConnection();
  qaAssert_(result && result.success, 'Connection test failed: ' + (result && result.message));

  return 'Connection OK, message=' + result.message + ', project=' + status.project;
}

function qaCheckCatalog_() {
  var schemas = listSchemas_();
  qaAssert_(schemas && typeof schemas.length === 'number', 'Schema list is not an array-like result');

  if (schemas.length === 0) {
    return 'Schema API returned 0 schemas';
  }

  var schemaName = schemas[0].name;
  var tables = listTables_(schemaName);
  qaAssert_(tables && typeof tables.length === 'number', 'Table list is not an array-like result');

  if (tables.length === 0) {
    return 'Schema ' + schemaName + ' returned 0 tables';
  }

  var table = tables[0];
  var detail = getTableSchema_(table.name, schemaName);
  qaAssert_(detail && detail.name, 'Table schema response is missing table name');
  qaAssert_(detail.columns && typeof detail.columns.length === 'number', 'Table schema response is missing columns');

  return 'Schema ' + schemaName +
    ', table ' + table.name +
    ', columns=' + detail.columns.length;
}

function qaCheckSimpleQuery_() {
  var result = executeSqlQuery_('SELECT 1 AS id;');
  qaAssert_(result && result.columns && result.columns.length >= 1, 'Simple query returned no columns');
  qaAssert_(result.rows && result.rows.length >= 1, 'Simple query returned no rows');

  return 'Simple query returned ' + result.rows.length + ' row(s)';
}

function qaCheckOdpsSignature_() {
  var config = getMcConfig_();
  var response = odpsFetch_({
    method: 'GET',
    host: config.endpoint,
    pathname: buildOdpsPath_(['projects', config.project, 'instances']),
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    securityToken: config.securityToken || null,
    project: config.project,
    query: { maxitems: '1' }
  });

  var code = response.getResponseCode();
  qaAssert_(code === 200, 'Signature probe returned HTTP ' + code);

  return 'Signature probe returned HTTP 200';
}


// ============================================================
// 运行所有测试
// ============================================================

/**
 * 运行所有数据目录测试
 */
function runAllCatalogTests() {
  Logger.log('========================================');
  Logger.log('运行所有数据目录测试');
  Logger.log('========================================\n');
  
  test_listSchemas();
  Logger.log('\n----------------------------------------\n');
  
  test_listTables();
  Logger.log('\n----------------------------------------\n');
  
  test_getTableSchema();
  Logger.log('\n----------------------------------------\n');
  
  test_listPartitions();
  Logger.log('\n----------------------------------------\n');
  
  test_getTableDetail();
  Logger.log('\n----------------------------------------\n');
  
  Logger.log('========================================');
  Logger.log('所有测试完成');
  Logger.log('========================================');
}

/**
 * 运行所有 SQL 测试
 */
function runAllSqlTests() {
  Logger.log('========================================');
  Logger.log('运行所有 SQL 测试');
  Logger.log('========================================\n');

  test_readOnlySqlGuard();
  Logger.log('\n----------------------------------------\n');

  test_endpointValidation();
  Logger.log('\n----------------------------------------\n');
  
  test_odpsSignature();
  Logger.log('\n----------------------------------------\n');
  
  test_connectionStatus();
  Logger.log('\n----------------------------------------\n');
  
  test_executeSimpleQuery();
  Logger.log('\n----------------------------------------\n');
  
  test_executeInvalidSql();
  Logger.log('\n----------------------------------------\n');
  
  Logger.log('========================================');
  Logger.log('所有测试完成');
  Logger.log('========================================');
}

/**
 * 运行所有测试
 */
function runAllTests() {
  runAllCatalogTests();
  Logger.log('\n\n');
  runAllSqlTests();
}
