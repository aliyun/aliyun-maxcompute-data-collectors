const assert = require('node:assert/strict');
const test = require('node:test');

const { loadGasContext, makeHttpResponse } = require('./helpers/gasHarness');

function b64(value) {
  return Buffer.from(value, 'utf8').toString('base64');
}

function plain(value) {
  return JSON.parse(JSON.stringify(value));
}

function resultXml(csv, descriptor) {
  const desc = descriptor || {
    Schema: {
      Columns: [
        { Name: 'id' },
        { Name: 'name' }
      ]
    }
  };
  return [
    '<Instance><Tasks><Task>',
    '<Status>Success</Status>',
    `<ResultDescriptor>${JSON.stringify(desc)}</ResultDescriptor>`,
    `<Result Transform="Base64">${b64(csv)}</Result>`,
    '</Task></Tasks></Instance>'
  ].join('');
}

test('on_open_builds_editor_addon_menu_with_expected_items', () => {
  const gas = loadGasContext({
    userProperties: { MC_LANGUAGE: 'en' }
  });

  gas.onOpen();

  assert.ok(gas.__uiCalls.some((call) => call[0] === 'createAddonMenu'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addItem' && call[3] === 'Open Query Panel' && call[4] === 'showSidebar'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addItem' && call[3] === 'Settings' && call[4] === 'showSettings'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addItem' && call[3] === 'Clear Current Sheet' && call[4] === 'clearCurrentSheet'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addSubMenu' && call[3] === 'Language'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addToUi' && call[1] === 'addon'));
});

test('on_install_delegates_to_on_open', () => {
  const gas = loadGasContext({
    userProperties: { MC_LANGUAGE: 'zh' }
  });

  gas.onInstall({});

  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addItem' && call[3] === '打开查询面板' && call[4] === 'showSidebar'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addItem' && call[3] === '设置连接' && call[4] === 'showSettings'));
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'addSubMenu' && call[3] === '语言'));
});

test('show_sidebar_redirects_to_settings_when_endpoint_is_invalid', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://example.com/api'
    }
  });

  gas.showSidebar();

  assert.ok(gas.__uiCalls.some((call) => call[0] === 'showSidebar'));
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('show_sidebar_initial_data_does_not_expose_endpoint', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_PROJECT: 'analytics_project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
      MC_LANGUAGE: 'en'
    }
  });

  gas.showSidebar();

  const template = gas.__htmlTemplates[0];
  assert.ok(template);
  const initialData = JSON.parse(template.initialData);
  assert.equal(initialData.connection.configured, true);
  assert.equal(Object.hasOwn(initialData.connection, 'project'), false);
  assert.equal(Object.hasOwn(initialData.connection, 'endpoint'), false);
});

test('clear_current_sheet_requires_confirmation_before_clearing', () => {
  const gas = loadGasContext();

  gas.clearCurrentSheet();

  const activeSheet = gas.__spreadsheet.getActiveSheet();
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'alert' && call[2].includes(activeSheet.getName())));
  assert.ok(activeSheet.calls.some((call) => call[0] === 'clear'));
});

test('clear_current_sheet_does_not_clear_when_cancelled', () => {
  const gas = loadGasContext({
    alertResponse: 'CANCEL'
  });

  gas.clearCurrentSheet();

  const activeSheet = gas.__spreadsheet.getActiveSheet();
  assert.ok(gas.__uiCalls.some((call) => call[0] === 'alert'));
  assert.equal(activeSheet.calls.some((call) => call[0] === 'clear'), false);
});

test('canonical_resource_sorts_query_params', () => {
  const gas = loadGasContext();
  assert.equal(gas.buildCanonicalResource_('/projects/p/instances', { b: '2', a: '1' }), '/projects/p/instances?a=1&b=2');
});

test('canonical_resource_keeps_empty_flag_params', () => {
  const gas = loadGasContext();
  assert.equal(gas.buildCanonicalResource_('/projects/p/instances/i', { result: '' }), '/projects/p/instances/i?result');
});

test('canonical_resource_encodes_path_and_query_parts', () => {
  const gas = loadGasContext();
  assert.equal(
    gas.buildCanonicalResource_('/projects/my project/tables/table/with space', { curr_schema: 's 1', prefix: 'a+b/c' }),
    '/projects/my%20project/tables/table/with%20space?curr_schema=s%201&prefix=a%2Bb%2Fc'
  );
});

test('canonical_resource_does_not_double_encode_existing_escapes', () => {
  const gas = loadGasContext();
  assert.equal(
    gas.buildCanonicalResource_('/projects/my%20project/tables/t%2F1', { prefix: 'a%2Bb' }),
    '/projects/my%20project/tables/t%2F1?prefix=a%2Bb'
  );
});

test('odps_path_builder_encodes_dynamic_segments_without_path_splitting', () => {
  const gas = loadGasContext();
  assert.equal(
    gas.buildOdpsPath_(['projects', 'proj/with space', 'tables', 'table/a']),
    '/projects/proj%2Fwith%20space/tables/table%2Fa'
  );
});

test('canonical_string_includes_odps_headers_sorted', () => {
  const gas = loadGasContext();
  const text = gas.buildCanonicalString_('get', {
    Date: 'Mon, 01 Jan 2024 00:00:00 GMT',
    'Content-MD5': '',
    'Content-Type': 'application/xml',
    'x-odps-zeta': 'z',
    'x-odps-alpha': 'a'
  }, '/projects/p', 'x-odps-');
  assert.match(text, /x-odps-alpha:a\nx-odps-zeta:z\n\/projects\/p$/);
});

test('header_lookup_is_case_insensitive', () => {
  const gas = loadGasContext();
  assert.equal(gas.getHeaderValue_({ Date: 'd', 'content-type': 'xml' }, 'Content-Type'), 'xml');
});

test('sql_job_xml_escapes_sql', () => {
  const gas = loadGasContext();
  const xml = gas.buildSqlJobXml_("select '<a&b>' as x", 'task');
  assert.match(xml, /&lt;a&amp;b&gt;/);
  assert.doesNotMatch(xml, /<a&b>/);
});

test('sql_job_xml_escapes_task_name', () => {
  const gas = loadGasContext();
  const xml = gas.buildSqlJobXml_('select 1', 'task<&>\'"');
  assert.match(xml, /<Name>task&lt;&amp;&gt;&apos;&quot;<\/Name>/);
});

test('audit_settings_are_task_settings_not_sql_prefix', () => {
  const gas = loadGasContext();
  const settings = gas.buildAuditSettings_({
    user: 'u@example.com',
    spreadsheetName: 'Book',
    spreadsheetId: 'sid',
    targetSheet: 'Out'
  });
  const xml = gas.buildSqlJobXml_('select 1;', 'task', settings);

  assert.match(xml, /<Config><Property><Name>settings<\/Name><Value>/);
  assert.match(xml, /&quot;EXT_PLATFORM_ID&quot;:&quot;Gsheet&quot;/);
  assert.match(xml, /&quot;EXT_NODE_ID&quot;:&quot;sid&quot;/);
  assert.match(xml, /&quot;EXT_NODE_NAME&quot;:&quot;Book&quot;/);
  assert.match(xml, /&quot;EXT_TASK_ID&quot;:&quot;Out&quot;/);
  assert.match(xml, /&quot;EXT_NODE_ONDUTY&quot;:&quot;u@example\.com&quot;/);
  assert.match(xml, /<Query>select 1;<\/Query>/);
  assert.doesNotMatch(xml, /<Query>SET EXT_/);
});

test('settings_parser_extracts_leading_set_statements', () => {
  const gas = loadGasContext();
  const parsed = gas.SettingsParser_.parse("SET A=B;\nSET odps.sql.mapper.split.size=256;\nSELECT `cds` FROM `california_schools`.`satscores` LIMIT 100");

  assert.deepEqual(plain(parsed.settings), {
    A: 'B',
    'odps.sql.mapper.split.size': '256'
  });
  assert.equal(parsed.sql, 'SELECT `cds` FROM `california_schools`.`satscores` LIMIT 100');
});

test('settings_parser_unquotes_string_values_and_ignores_semicolons_in_literals', () => {
  const gas = loadGasContext();
  const parsed = gas.SettingsParser_.parse("SET x='a;b'; SET y=\"c\\\"d\"; SELECT ';' AS semi;");

  assert.deepEqual(plain(parsed.settings), {
    x: 'a;b',
    y: 'c"d'
  });
  assert.equal(parsed.sql, "SELECT ';' AS semi;");
});

test('settings_parser_rejects_bad_or_reserved_settings', () => {
  const gas = loadGasContext();

  assert.throws(() => gas.SettingsParser_.parse('SET bad key=1; SELECT 1'), /格式不正确/);
  assert.throws(() => gas.SettingsParser_.parse('SET EXT_NODE_ID=other; SELECT 1'), /EXT_\* 审计字段/);
});

test('audit_settings_empty_without_context', () => {
  const gas = loadGasContext();
  assert.deepEqual(plain(gas.buildAuditSettings_(null)), {});
  assert.doesNotMatch(gas.buildSqlJobXml_('select 1;', 'task', null), /<Config>/);
});

test('audit_node_name_contains_spreadsheet_name_when_available', () => {
  const gas = loadGasContext();
  const settings = gas.buildAuditSettings_({
    user: 'u@example.com',
    spreadsheetName: 'Revenue',
    spreadsheetId: 'sid',
    targetSheet: 'Daily'
  });
  assert.equal(settings.EXT_NODE_NAME, 'Revenue');

  const withoutName = gas.buildAuditSettings_({
    user: 'u@example.com',
    spreadsheetId: 'sid',
    targetSheet: 'Daily'
  });
  assert.equal(Object.hasOwn(withoutName, 'EXT_NODE_NAME'), false);
});

test('audit_values_keep_quotes_inside_json_settings', () => {
  const gas = loadGasContext();
  const settings = gas.buildAuditSettings_({
    user: "o'hara@example.com",
    spreadsheetName: "CEO's Book",
    spreadsheetId: 'sid',
    targetSheet: "Today's Data"
  });
  assert.equal(settings.EXT_NODE_NAME, "CEO's Book");
  assert.equal(settings.EXT_NODE_ONDUTY, "o'hara@example.com");
});

test('audit_values_strip_newlines_tabs', () => {
  const gas = loadGasContext();
  const settings = gas.buildAuditSettings_({
    user: 'u\n@example.com',
    spreadsheetName: 'Book\nName',
    spreadsheetId: 'sid',
    targetSheet: 'Sheet\tOne'
  });
  assert.equal(settings.EXT_NODE_NAME, 'Book Name');
  assert.equal(settings.EXT_TASK_ID, 'Sheet One');
  assert.equal(settings.EXT_NODE_ONDUTY, 'u @example.com');
});

test('audit_node_id_uses_sheet_id_with_length_cap', () => {
  const gas = loadGasContext();
  const nodeId = gas.buildAuditNodeId_('s'.repeat(120));
  assert.equal(nodeId.length, 64);
  assert.equal(nodeId, 's'.repeat(64));
});

test('audit_field_lengths_are_capped', () => {
  const gas = loadGasContext();
  const settings = gas.buildAuditSettings_({
    user: 'u'.repeat(200),
    spreadsheetName: 'b'.repeat(200),
    spreadsheetId: 's'.repeat(200),
    targetSheet: 't'.repeat(200)
  });
  assert.equal(settings.EXT_PLATFORM_ID.length <= 32, true);
  assert.equal(settings.EXT_NODE_ID.length <= 64, true);
  assert.equal(settings.EXT_NODE_NAME.length <= 128, true);
  assert.equal(settings.EXT_TASK_ID.length <= 64, true);
  assert.equal(settings.EXT_NODE_ONDUTY.length <= 64, true);
});

test('audit_task_name_is_sanitized', () => {
  const gas = loadGasContext();
  const taskName = gas.buildAuditTaskName_({ targetSheet: '收入 Sheet #1' });
  assert.match(taskName, /^query_task_/);
  assert.match(taskName, /^[A-Za-z0-9_]+$/);
  assert.equal(taskName.length <= 64, true);
});

test('safe_script_json_escapes_script_breakout_sequences', () => {
  const gas = loadGasContext();
  const json = gas.toSafeScriptJson_({
    sheetNames: ['</script><script>alert(1)</script>', 'A&B'],
    text: '\u2028\u2029'
  });

  assert.doesNotMatch(json, /<\/script>/i);
  assert.match(json, /\\u003c\/script\\u003e/);
  assert.match(json, /\\u0026/);
  assert.deepEqual(JSON.parse(json), {
    sheetNames: ['</script><script>alert(1)</script>', 'A&B'],
    text: '\u2028\u2029'
  });
});

test('config_for_ui_does_not_return_secret_values', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'akid123456',
      ALIYUN_ACCESS_KEY_SECRET: 'secret123456',
      MC_SECURITY_TOKEN: 'token123456',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  const cfg = gas.getMcConfigForUi();
  assert.equal(cfg.accessKeyId, 'akid123456');
  assert.equal(cfg.accessKeySecret, '');
  assert.equal(cfg.accessKeySecretConfigured, true);
  assert.equal(cfg.accessKeySecretMasked, 'secr****3456');
  assert.equal(cfg.securityToken, '');
  assert.equal(cfg.securityTokenConfigured, true);
  assert.equal(cfg.securityTokenMasked, 'toke****3456');
});

test('config_reads_user_properties_before_legacy_script_properties', () => {
  const gas = loadGasContext({
    scriptProperties: {
      ALIYUN_ACCESS_KEY_ID: 'script-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'script-secret',
      MC_PROJECT: 'script-project',
      MC_ENDPOINT: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api'
    },
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'user-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'user-secret',
      MC_PROJECT: 'user-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  const cfg = gas.getMcConfig_();
  assert.equal(cfg.accessKeyId, 'user-ak');
  assert.equal(cfg.accessKeySecret, 'user-secret');
  assert.equal(cfg.project, 'user-project');
  assert.equal(cfg.endpoint, 'https://service.ap-southeast-1.maxcompute.aliyun.com/api');
});

test('config_empty_user_property_overrides_legacy_script_property', () => {
  const gas = loadGasContext({
    scriptProperties: {
      ALIYUN_ACCESS_KEY_ID: 'script-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'script-secret',
      MC_PROJECT: 'script-project',
      MC_ENDPOINT: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api'
    },
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: '',
      ALIYUN_ACCESS_KEY_SECRET: '',
      MC_PROJECT: '',
      MC_ENDPOINT: ''
    }
  });

  const cfg = gas.getMcConfig_();
  assert.equal(cfg.accessKeyId, '');
  assert.equal(cfg.accessKeySecret, '');
  assert.equal(cfg.project, '');
  assert.equal(cfg.endpoint, '');
});

test('config_falls_back_to_legacy_script_properties', () => {
  const gas = loadGasContext({
    scriptProperties: {
      ALIYUN_ACCESS_KEY_ID: 'script-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'script-secret',
      MC_PROJECT: 'script-project',
      MC_ENDPOINT: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api'
    },
    userProperties: {}
  });

  const cfg = gas.getMcConfig_();
  assert.equal(cfg.accessKeyId, 'script-ak');
  assert.equal(cfg.accessKeySecret, 'script-secret');
  assert.equal(cfg.project, 'script-project');
  assert.equal(cfg.endpoint, 'https://service.cn-hangzhou.maxcompute.aliyun.com/api');
});

test('save_config_keeps_existing_secret_when_secret_input_is_blank', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'old-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
      MC_SECURITY_TOKEN: 'old-token',
      MC_PROJECT: 'old-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  gas.saveMcConfig({
    accessKeyId: 'new-ak',
    accessKeySecret: '',
    project: 'new-project',
    endpoint: 'https://service.cn-shanghai.maxcompute.aliyun.com/api',
    securityToken: ''
  });

  assert.deepEqual(gas.__userProperties.getProperties(), {
    ALIYUN_ACCESS_KEY_ID: 'new-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_SECURITY_TOKEN: 'old-token',
    MC_PROJECT: 'new-project',
    MC_ENDPOINT: 'https://service.cn-shanghai.maxcompute.aliyun.com/api'
  });
});

test('save_config_can_clear_existing_security_token', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_SECURITY_TOKEN: 'old-token',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: '',
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    securityToken: '',
    clearSecurityToken: true
  });

  assert.equal(gas.__userProperties.getProperty('MC_SECURITY_TOKEN'), null);
  assert.equal(gas.__userProperties.getProperty('MC_SECURITY_TOKEN_CLEARED'), 'true');
  assert.equal(gas.getMcConfig_().securityToken, '');
});

test('save_config_clear_token_overrides_legacy_script_token_fallback', () => {
  const gas = loadGasContext({
    scriptProperties: {
      ALIYUN_ACCESS_KEY_ID: 'script-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'script-secret',
      MC_SECURITY_TOKEN: 'script-token',
      MC_PROJECT: 'script-project',
      MC_ENDPOINT: 'https://service.cn-hangzhou.maxcompute.aliyun.com/api'
    },
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: '',
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    securityToken: '',
    clearSecurityToken: true
  });

  assert.equal(gas.__userProperties.getProperty('MC_SECURITY_TOKEN'), null);
  assert.equal(gas.__userProperties.getProperty('MC_SECURITY_TOKEN_CLEARED'), 'true');
  assert.equal(gas.getMcConfig_().securityToken, '');
});

test('save_config_rejects_non_maxcompute_https_endpoint', () => {
  const gas = loadGasContext();

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'http://service.ap-southeast-1.maxcompute.aliyun.com/api'
  }), /Endpoint 格式不正确/);

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://example.com/api'
  }), /Endpoint 格式不正确/);
});

test('save_config_rejects_overlong_config_values_without_persisting', () => {
  const gas = loadGasContext();

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'a'.repeat(129),
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  }), /AccessKey ID 长度超过限制/);
  assert.deepEqual(gas.__userProperties.getProperties(), {});

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 's'.repeat(257),
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  }), /AccessKey Secret 长度超过限制/);

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'p'.repeat(129),
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  }), /Project 长度超过限制/);

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://service.' + 'a'.repeat(230) + '.maxcompute.aliyun.com/api'
  }), /Endpoint 长度超过限制/);

  assert.throws(() => gas.saveMcConfig({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    securityToken: 't'.repeat(4097)
  }), /Security Token 长度超过限制/);
});

test('test_connection_rejects_invalid_endpoint_without_persisting', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'old-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
      MC_PROJECT: 'old-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });

  const result = gas.testMcConnection({
    accessKeyId: 'ak',
    accessKeySecret: 'secret',
    project: 'proj',
    endpoint: 'https://example.com/api'
  });

  assert.equal(result.success, false);
  assert.match(result.message, /Endpoint 格式不正确/);
  assert.deepEqual(gas.__userProperties.getProperties(), {
    ALIYUN_ACCESS_KEY_ID: 'old-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_PROJECT: 'old-project',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  });
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('runtime_connection_rejects_invalid_saved_endpoint_before_http', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://example.com/api'
    }
  });

  const result = gas.testConnection();
  assert.equal(result.success, false);
  assert.match(result.message, /Endpoint 格式不正确/);
  const status = gas.getConnectionStatus();
  assert.equal(status.configured, false);
  assert.match(status.error, /Endpoint 格式不正确/);
  assert.throws(() => gas.submitQuery('select 1'), /Endpoint 格式不正确/);
  assert.throws(() => gas.getQueryProgress('inst-1'), /Endpoint 格式不正确/);
  assert.throws(() => gas.writeQueryResult('inst-1', 'Invalid Endpoint Result'), /Endpoint 格式不正确/);
  const cancelResult = gas.cancelQuery('inst-1');
  assert.match(cancelResult.killResult, /^failed:exception:.*Endpoint 格式不正确/);
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('catalog_entrypoints_reject_invalid_saved_endpoint_before_http', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'secret',
      MC_PROJECT: 'proj',
      MC_ENDPOINT: 'https://example.com/api'
    }
  });

  assert.throws(() => gas.getSchemas(), /Endpoint 格式不正确/);
  assert.throws(() => gas.getTables('s1'), /Endpoint 格式不正确/);
  assert.throws(() => gas.getTableDetail('t1', 's1'), /Endpoint 格式不正确/);
  assert.throws(() => gas.getPartitions('t1', 's1'), /Endpoint 格式不正确/);
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('test_connection_restores_original_config_after_success', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'old-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
      MC_SECURITY_TOKEN: 'old-token',
      MC_PROJECT: 'old-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas></Schemas>'));

  const result = gas.testMcConnection({
    accessKeyId: 'new-ak',
    accessKeySecret: 'new-secret',
    project: 'new-project',
    endpoint: 'https://service.cn-shanghai.maxcompute.aliyun.com/api',
    securityToken: ''
  });

  assert.equal(result.success, true);
  assert.equal(result.message, 'Connection OK (Schemas: 0)');
  assert.doesNotMatch(result.message, /new-project/);
  assert.deepEqual(gas.__userProperties.getProperties(), {
    ALIYUN_ACCESS_KEY_ID: 'old-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_SECURITY_TOKEN: 'old-token',
    MC_PROJECT: 'old-project',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  });
  assert.match(gas.__urlFetchCalls[0].url, /service\.cn-shanghai\.maxcompute\.aliyun\.com/);
});

test('test_connection_restores_original_config_after_failure', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'old-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
      MC_PROJECT: 'old-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(403, '<Error><Code>Forbidden</Code><Message>denied on sensitive_project_name sensitive_schema</Message></Error>'));

  const result = gas.testMcConnection({
    accessKeyId: 'new-ak',
    accessKeySecret: 'new-secret',
    project: 'new-project',
    endpoint: 'https://service.cn-shanghai.maxcompute.aliyun.com/api'
  });

  assert.equal(result.success, false);
  assert.match(result.message, /Forbidden: denied on sensitive_project_name sensitive_schema/);
  assert.deepEqual(gas.__userProperties.getProperties(), {
    ALIYUN_ACCESS_KEY_ID: 'old-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_PROJECT: 'old-project',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
  });
});

test('test_connection_surfaces_catalog_parse_failure_message', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<SensitiveSchemas><Schema><Name>sensitive_schema</Name></Schema></SensitiveSchemas>'));

  const result = gas.testConnection();

  assert.equal(result.success, false);
  assert.match(result.message, /Connection failed: 解析 Schema 列表 XML 失败: Unexpected XML root/);
});

test('connection_error_message_passes_through_catalog_errors_unchanged', () => {
  const gas = loadGasContext();
  const passthrough = gas.getConnectionErrorMessage_(
    new Error('获取 Schema 列表失败 (HTTP 403): denied on sensitive_schema'),
    false
  );

  assert.equal(passthrough, '获取 Schema 列表失败 (HTTP 403): denied on sensitive_schema');
});

test('test_connection_preserves_unrelated_user_properties_after_success', () => {
  const originalUserProperties = {
    ALIYUN_ACCESS_KEY_ID: 'old-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_SECURITY_TOKEN: 'old-token',
    MC_PROJECT: 'old-project',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    MC_LANGUAGE: 'zh',
    UNRELATED_USER_PROPERTY: 'keep-me'
  };
  const gas = loadGasContext({
    userProperties: originalUserProperties
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas></Schemas>'));

  const result = gas.testMcConnection({
    accessKeyId: 'new-ak',
    accessKeySecret: 'new-secret',
    project: 'new-project',
    endpoint: 'https://service.cn-shanghai.maxcompute.aliyun.com/api',
    securityToken: ''
  });

  assert.equal(result.success, true);
  assert.deepEqual(gas.__userProperties.getProperties(), originalUserProperties);
});

test('test_connection_preserves_unrelated_user_properties_after_failure', () => {
  const originalUserProperties = {
    ALIYUN_ACCESS_KEY_ID: 'old-ak',
    ALIYUN_ACCESS_KEY_SECRET: 'old-secret',
    MC_PROJECT: 'old-project',
    MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    MC_LANGUAGE: 'zh',
    UNRELATED_USER_PROPERTY: 'keep-me'
  };
  const gas = loadGasContext({
    userProperties: originalUserProperties
  });
  gas.__urlFetchQueue.push(makeHttpResponse(403, '<Error><Code>Forbidden</Code><Message>denied</Message></Error>'));

  const result = gas.testMcConnection({
    accessKeyId: 'new-ak',
    accessKeySecret: 'new-secret',
    project: 'new-project',
    endpoint: 'https://service.cn-shanghai.maxcompute.aliyun.com/api'
  });

  assert.equal(result.success, false);
  assert.deepEqual(gas.__userProperties.getProperties(), originalUserProperties);
});

test('test_connection_with_legacy_script_config_does_not_persist_user_config', () => {
  const gas = loadGasContext({
    scriptProperties: {
      ALIYUN_ACCESS_KEY_ID: 'script-ak',
      ALIYUN_ACCESS_KEY_SECRET: 'script-secret',
      MC_PROJECT: 'script-project',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    },
    userProperties: {}
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas></Schemas>'));

  const result = gas.testMcConnection({
    accessKeyId: '',
    accessKeySecret: '',
    project: '',
    endpoint: ''
  });

  assert.equal(result.success, true);
  assert.deepEqual(gas.__userProperties.getProperties(), {});
  assert.match(gas.__urlFetchCalls[0].url, /service\.ap-southeast-1\.maxcompute\.aliyun\.com/);
});

test('connection_status_exposes_only_sidebar_needed_connection_fields', () => {
  const gas = loadGasContext();
  const status = gas.getConnectionStatus();
  assert.equal(status.configured, true);
  assert.equal(Object.hasOwn(status, 'userKey'), false);
  assert.equal(Object.hasOwn(status, 'endpoint'), false);
  assert.equal(Object.hasOwn(status, 'project'), false);
});

test('audit_user_key_uses_active_user_email_for_submitter_audit', () => {
  const gas = loadGasContext({
    activeUserEmail: 'runner@example.com'
  });

  assert.equal(gas.getCurrentUserAuditKey_(), 'runner@example.com');
  assert.equal(gas.__userProperties.getProperty('UNRELATED_USER_PROPERTY'), null);
});

test('audit_user_key_falls_back_to_unknown_when_email_is_unavailable', () => {
  const gas = loadGasContext({
    activeUserEmail: '',
    oauthToken: ''
  });

  assert.equal(gas.getCurrentUserAuditKey_(), 'unknown');
});

test('audit_user_key_falls_back_to_google_userinfo_email', () => {
  const gas = loadGasContext({
    activeUserEmail: ''
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '{"email":"userinfo@example.com"}'));

  assert.equal(gas.getCurrentUserAuditKey_(), 'userinfo@example.com');
  assert.equal(gas.__urlFetchCalls[0].url, 'https://www.googleapis.com/oauth2/v2/userinfo');
  assert.equal(gas.__urlFetchCalls[0].options.headers.Authorization, 'Bearer oauth-token');
  assert.equal(gas.__urlFetchCalls[0].options.headers['User-Agent'], 'Google Sheet Plugin');
});

test('resolve_timeout_ms_bounds_user_supplied_sync_timeout', () => {
  const gas = loadGasContext();

  assert.equal(gas.resolveTimeoutMs_(undefined), 300000);
  assert.equal(gas.resolveTimeoutMs_('bad'), 300000);
  assert.equal(gas.resolveTimeoutMs_(-10), 300000);
  assert.equal(gas.resolveTimeoutMs_(0), 300000);
  assert.equal(gas.resolveTimeoutMs_(0.5), 300000);
  assert.equal(gas.resolveTimeoutMs_(1), 1000);
  assert.equal(gas.resolveTimeoutMs_(30), 30000);
  assert.equal(gas.resolveTimeoutMs_(9999), 300000);
});

test('read_only_sql_allows_only_select_and_with_dql', () => {
  const gas = loadGasContext();
  assert.doesNotThrow(() => gas.assertReadOnlySql_('select * from t limit 1'));
  assert.doesNotThrow(() => gas.assertReadOnlySql_('with c as (select 1) select * from c'));
});

test('read_only_sql_rejects_non_dql_metadata_and_explain', () => {
  const gas = loadGasContext();
  const rejected = [
    'show tables',
    'show create table t',
    'show grants',
    'desc t',
    'describe t',
    'explain select * from t'
  ];
  for (const sql of rejected) {
    assert.throws(() => gas.assertReadOnlySql_(sql), /不支持以 [A-Z_]+ 开头的 SQL|SELECT \/ WITH 只读查询/);
  }
});

test('read_only_sql_allows_leading_set_statements', () => {
  const gas = loadGasContext();
  assert.doesNotThrow(() => gas.assertReadOnlySql_("set odps.sql.mapper.split.size=256; set x='insert into t'; select * from t"));
});

test('read_only_sql_rejects_overlong_sql_before_http_request', () => {
  const gas = loadGasContext();
  const overlongSql = 'select 1 -- ' + 'x'.repeat(65536);

  assert.throws(() => gas.assertReadOnlySql_(overlongSql), /SQL 长度超过限制/);
  assert.throws(() => gas.submitSqlJobOnly_(overlongSql), /SQL 长度超过限制/);
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('read_only_sql_rejects_user_supplied_reserved_audit_set_statements', () => {
  const gas = loadGasContext();
  const reserved = [
    "set EXT_PLATFORM_ID='other'; select 1",
    "set ext_node_id='node'; select 1",
    "set ext_task_id='task'; select 1",
    "set ext_node_name='name'; select 1",
    "set ext_node_onduty='owner'; select 1",
    "set ext_dagtype='2'; select 1"
  ];

  for (const sql of reserved) {
    assert.throws(() => gas.assertReadOnlySql_(sql), /EXT_\* 审计字段/);
  }

  assert.doesNotThrow(() => gas.assertReadOnlySql_("set x='EXT_PLATFORM_ID'; select 1"));
});

test('read_only_sql_rejects_ddl_and_dml', () => {
  const gas = loadGasContext();
  const forbidden = [
    'insert into t select * from s',
    'insert overwrite table t select * from s',
    'update t set a = 1',
    'delete from t where id = 1',
    'merge into t using s on t.id = s.id when matched then update set a = s.a',
    'create table t (id bigint)',
    'alter table t add columns (name string)',
    'drop table t',
    'truncate table t',
    'grant select on table t to user u',
    'revoke select on table t from user u',
    'load data inpath "/tmp/a" into table t',
    'unload from table t into location "/tmp/a"',
    'analyze table t compute statistics',
    'call some_proc()',
    'use other_project',
    'begin',
    'commit',
    'rollback'
  ];
  for (const sql of forbidden) {
    assert.throws(() => gas.assertReadOnlySql_(sql), /仅允许提交只读查询|不支持 DDL\/DML/);
  }
});

test('read_only_sql_rejects_multiple_non_set_statements', () => {
  const gas = loadGasContext();
  assert.throws(() => gas.assertReadOnlySql_('select 1; select 2'), /每次仅允许提交一条只读查询/);
  assert.throws(() => gas.assertReadOnlySql_('select 1; set x=1'), /SET 语句只能放在只读查询之前/);
});

test('read_only_sql_rejects_dml_hidden_under_with', () => {
  const gas = loadGasContext();
  assert.throws(() => gas.assertReadOnlySql_('with c as (select * from s) insert into t select * from c'), /仅允许提交只读查询/);
});

test('read_only_sql_rejects_explain_at_first_keyword', () => {
  const gas = loadGasContext();
  const explainCases = [
    'explain select * from t',
    'explain insert into t select * from s',
    'explain create materialized view mv as select 1',
    'explain create external table t (id bigint)',
    'explain add file "oss://bucket/path.jar"',
    'explain install package p',
    'explain msck repair table t'
  ];
  for (const sql of explainCases) {
    assert.throws(() => gas.assertReadOnlySql_(sql), /不支持以 EXPLAIN 开头的 SQL/);
  }
});

test('read_only_sql_ignores_keywords_inside_comments_and_literals', () => {
  const gas = loadGasContext();
  assert.doesNotThrow(() => gas.assertReadOnlySql_("select 'drop table t' as text, \"insert into t\" as text2 from t -- delete from x"));
  assert.doesNotThrow(() => gas.assertReadOnlySql_('select * from t /* truncate table x; */ where name = "grant"'));
  assert.doesNotThrow(() => gas.assertReadOnlySql_('select * from `drop` where action = "delete from t"'));
});

test('submit_sql_rejects_forbidden_sql_before_http_request', () => {
  const gas = loadGasContext();
  assert.throws(() => gas.submitSqlJobOnly_('drop table t'), /仅允许提交只读查询/);
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('sql_executor_logs_raw_sql_text_for_traceability', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/inst-1' }));

  gas.submitSqlJobOnly_("select 'customer@example.com' as email, secret_col from sensitive_table");

  const logText = gas.__logs.join('\n');
  assert.match(logText, /sql=select 'customer@example\.com' as email, secret_col from sensitive_table/);
});

test('parse_instance_status_success', () => {
  const gas = loadGasContext();
  assert.deepEqual(plain(gas.parseInstanceStatusXml_('<Instance><Name>i1</Name><Status>Terminated</Status></Instance>')), {
    instanceId: 'i1',
    status: 'Terminated'
  });
});

test('parse_task_status_success', () => {
  const gas = loadGasContext();
  assert.deepEqual(plain(gas.parseTaskStatusXml_('<Instance><Tasks><Task><Name>t1</Name><Status>Success</Status></Task></Tasks></Instance>')), {
    taskName: 't1',
    taskStatus: 'Success'
  });
});

test('parse_result_xml_base64', () => {
  const gas = loadGasContext();
  const parsed = gas.parseInstanceResultXml_(resultXml('id,name\n1,ok\n2,\\N\n'));
  assert.deepEqual(plain(parsed.columns), ['id', 'name']);
  assert.deepEqual(plain(parsed.rows), [['1', 'ok'], ['2', '']]);
  assert.equal(parsed.rowCount, 2);
});

test('parse_failed_result_xml', () => {
  const gas = loadGasContext();
  const xml = `<Instance><Tasks><Task><Status>Failed</Status><Result Transform="Base64">${b64('bad sql')}</Result></Task></Tasks></Instance>`;
  const parsed = gas.parseInstanceResultXml_(xml);
  assert.equal(parsed.taskStatus, 'Failed');
  assert.equal(parsed.rawResult, 'bad sql');
});

test('parse_error_xml_extracts_message', () => {
  const gas = loadGasContext();
  assert.equal(gas.parseErrorXml_('<Error><Code>InvalidArgument</Code><Message>bad request</Message></Error>'), 'InvalidArgument: bad request');
  assert.equal(gas.parseErrorSummary_('<Error><Code>InvalidArgument</Code><Message>bad request</Message></Error>'), 'InvalidArgument: bad request');
});

test('logview_url_extracts_region', () => {
  const gas = loadGasContext();
  assert.equal(
    gas.buildLogviewUrl_('inst1'),
    'https://maxcompute.console.aliyun.com/ap-southeast-1/job-insights?h=https%3A%2F%2Fservice.ap-southeast-1.maxcompute.aliyun.com%2Fapi&p=proj&i=inst1'
  );
});

test('logview_url_encodes_query_params', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'sk',
      MC_PROJECT: 'proj with space',
      MC_ENDPOINT: 'https://service.cn-shanghai.maxcompute.aliyun.com/api'
    }
  });
  assert.equal(
    gas.buildLogviewUrl_('inst/1?x'),
    'https://maxcompute.console.aliyun.com/cn-shanghai/job-insights?h=https%3A%2F%2Fservice.cn-shanghai.maxcompute.aliyun.com%2Fapi&p=proj%20with%20space&i=inst%2F1%3Fx'
  );
});

test('odps_fetch_does_not_mutate_query_object', () => {
  const gas = loadGasContext();
  const query = { result: '' };
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  gas.odpsFetch_({
    method: 'GET',
    host: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    pathname: '/projects/proj/instances/inst-1',
    accessKeyId: 'ak',
    accessKeySecret: 'sk',
    project: 'proj',
    query
  });

  assert.deepEqual(query, { result: '' });
  assert.match(gas.__urlFetchCalls[0].url, /curr_project=proj/);
});

test('odps_fetch_sends_google_sheet_plugin_user_agent', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance></Instance>'));

  gas.odpsFetch_({
    method: 'GET',
    host: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    pathname: '/projects/proj/instances',
    accessKeyId: 'ak',
    accessKeySecret: 'sk',
    project: 'proj'
  });

  assert.equal(gas.__urlFetchCalls[0].options.headers['User-Agent'], 'Google Sheet Plugin');
});

test('odps_fetch_logs_safe_resource_type_without_full_url_or_business_ids', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table></Table>'));

  gas.odpsFetch_({
    method: 'GET',
    host: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    pathname: '/projects/sensitive_project/tables/customer_orders',
    accessKeyId: 'ak',
    accessKeySecret: 'sk',
    project: 'sensitive_project',
    query: { curr_schema: 'finance_schema', prefix: 'customer' }
  });

  const logText = gas.__logs.join('\n');
  assert.match(logText, /host=service\.ap-southeast-1\.maxcompute\.aliyun\.com\/api/);
  assert.match(logText, /resource=table/);
  assert.doesNotMatch(logText, /sensitive_project/);
  assert.doesNotMatch(logText, /customer_orders/);
  assert.doesNotMatch(logText, /finance_schema/);
  assert.doesNotMatch(logText, /\?curr_schema=/);
});

test('odps_fetch_logs_x_odps_request_id_from_error_response', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(400, '<Error></Error>', {
    'x-odps-request-id': 'req-123:abc'
  }));

  gas.odpsFetch_({
    method: 'GET',
    host: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    pathname: '/projects/proj/instances',
    accessKeyId: 'ak',
    accessKeySecret: 'sk',
    project: 'proj'
  });

  assert.match(gas.__logs.join('\n'), /\[odpsFetch\] HTTP 400 requestId=req-123:abc/);
});

test('odps_fetch_ignores_non_odps_request_id_headers', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(400, '<Error></Error>', {
    'x-acs-request-id': 'acs-req',
    'x-request-id': 'generic-req'
  }));

  gas.odpsFetch_({
    method: 'GET',
    host: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api',
    pathname: '/projects/proj/instances',
    accessKeyId: 'ak',
    accessKeySecret: 'sk',
    project: 'proj'
  });

  assert.match(gas.__logs.join('\n'), /\[odpsFetch\] HTTP 400$/);
  assert.doesNotMatch(gas.__logs.join('\n'), /acs-req|generic-req/);
});

test('submit_sql_posts_to_instances', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/inst-1' }));
  gas.submitSqlJobOnly_('select 1', { spreadsheetName: 'Book', spreadsheetId: 'sid', targetSheet: 'Out', user: 'u@example.com' });
  assert.equal(gas.__urlFetchCalls.length, 1);
  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj\/instances\?curr_project=proj$/);
  assert.equal(gas.__urlFetchCalls[0].options.method, 'post');
});

test('submit_sql_body_contains_audit_ext_fields', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/inst-1' }));
  gas.submitSqlJobOnly_('select 1', { spreadsheetName: 'Book', spreadsheetId: 'sid', targetSheet: 'Out', user: 'u@example.com' });
  const body = gas.__urlFetchCalls[0].options.payload;
  assert.match(body, /<Config><Property><Name>settings<\/Name><Value>/);
  assert.match(body, /&quot;EXT_PLATFORM_ID&quot;:&quot;Gsheet&quot;/);
  assert.match(body, /&quot;EXT_NODE_ID&quot;:&quot;sid&quot;/);
  assert.match(body, /&quot;EXT_NODE_NAME&quot;:&quot;Book&quot;/);
  assert.match(body, /&quot;EXT_TASK_ID&quot;:&quot;Out&quot;/);
  assert.match(body, /&quot;EXT_NODE_ONDUTY&quot;:&quot;u@example\.com&quot;/);
  assert.match(body, /<Query>select 1<\/Query>/);
  assert.doesNotMatch(body, /<Query>SET EXT_/);
});

test('submit_sql_moves_leading_set_statements_into_task_settings', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/inst-1' }));

  gas.submitSqlJobOnly_("SET A=B;\nSET odps.sql.mapper.split.size=256;\nSELECT `cds` FROM `california_schools`.`satscores` LIMIT 100", {
    spreadsheetName: 'Book',
    spreadsheetId: 'sid',
    targetSheet: 'Out',
    user: 'u@example.com'
  });

  const body = gas.__urlFetchCalls[0].options.payload;
  assert.match(body, /&quot;A&quot;:&quot;B&quot;/);
  assert.match(body, /&quot;odps.sql.mapper.split.size&quot;:&quot;256&quot;/);
  assert.match(body, /&quot;EXT_PLATFORM_ID&quot;:&quot;Gsheet&quot;/);
  assert.match(body, /<Query>SELECT `cds` FROM `california_schools`\.`satscores` LIMIT 100<\/Query>/);
  assert.doesNotMatch(body, /<Query>SET A=B/);
});

test('submit_sql_201_extracts_instance_id', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/inst-1' }));
  assert.deepEqual(plain(gas.submitSqlJobOnly_('select 1')), { instanceId: 'inst-1' });
});

test('submit_sql_201_rejects_invalid_location_instance_id', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/bad?x=1' }));

  assert.throws(() => gas.submitSqlJobOnly_('select 1'), /Instance ID 格式不正确/);
});

test('submit_query_sync_result_writes_sheet_and_returns_summary', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  const summary = gas.submitQuery('select id, name from t', null, 'Sync Result');
  assert.equal(summary.sync, true);
  assert.equal(summary.rowCount, 1);
  assert.equal(summary.columnCount, 2);
  assert.equal(summary.sheetName, 'Sync Result');

  const sheet = gas.__spreadsheet.getSheetByName('Sync Result');
  assert.deepEqual(sheet.values[0], ['id', 'name']);
  assert.deepEqual(sheet.values[1], ['1', 'ok']);
});

test('code_entry_logs_raw_context_fields_for_traceability', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'sk',
      MC_PROJECT: 'sensitive_project_name',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    },
    spreadsheetOptions: {
      name: 'Sensitive Workbook',
      id: 'spreadsheet-secret-id'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  gas.submitQuery('select id, name from t', null, 'Sensitive Sheet');

  const logText = gas.__logs.join('\n');
  assert.match(logText, /user=runner@example\.com/);
  assert.match(logText, /spreadsheetName=Sensitive Workbook/);
  assert.match(logText, /spreadsheetId=spreadsheet-secret-id/);
  assert.match(logText, /targetSheet=Sensitive Sheet/);
  assert.match(logText, /project=sensitive_project_name/);
  assert.match(logText, /sql=select id, name from t/);
});

test('submit_sql_non_2xx_throws_parsed_error', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(400, '<Error><Code>BadRequest</Code><Message>bad sql on sensitive_table</Message></Error>', {
    'x-odps-request-id': 'req-submit-1'
  }));
  assert.throws(
    () => gas.submitSqlJobOnly_('select broken'),
    (err) => {
      assert.match(err.message, /requestId=req-submit-1/);
      assert.match(err.message, /BadRequest: bad sql on sensitive_table/);
      return true;
    }
  );
  assert.match(gas.__logs.join('\n'), /\[SqlExecutor\] submitSqlJob HTTP 400 requestId=req-submit-1/);
});

test('submit_sql_http_400_logs_full_request_body_for_diagnostics', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(400, '<Error><Code>ParseError</Code><Message>bad request</Message></Error>'));

  assert.throws(
    () => gas.submitSqlJobOnly_("select '<bad&xml>' as raw_value", {
      spreadsheetName: 'Book',
      spreadsheetId: 'sid',
      targetSheet: 'Out',
      user: 'u@example.com'
    }),
    /提交作业失败/
  );

  const logText = gas.__logs.join('\n');
  assert.match(logText, /\[SqlExecutor\] submitSqlJob HTTP 400 requestBody=<Instance>/);
  assert.match(logText, /<Config><Property><Name>settings<\/Name><Value>/);
  assert.match(logText, /&quot;EXT_PLATFORM_ID&quot;:&quot;Gsheet&quot;/);
  assert.match(logText, /select &apos;&lt;bad&amp;xml&gt;&apos; as raw_value/);
  assert.doesNotMatch(logText, /<Query>SET EXT_/);
});

test('submit_sql_non_400_error_does_not_log_request_body', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(403, '<Error><Code>Forbidden</Code><Message>denied</Message></Error>'));

  assert.throws(
    () => gas.submitSqlJobOnly_('select sensitive_col from sensitive_table'),
    /提交作业失败/
  );

  const logText = gas.__logs.join('\n');
  assert.doesNotMatch(logText, /requestBody=/);
});

test('execute_sql_async_logs_raw_instance_id_for_traceability', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/sensitive-instance-id' }));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-instance-id</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-instance-id</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Name>query_task</Name><Status>Success</Status></Task></Tasks></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  gas.executeSqlQuery_('select id, name from t');

  const logText = gas.__logs.join('\n');
  assert.match(logText, /instanceId=sensitive-instance-id/);
});

test('execute_sql_failed_task_throws_safe_error_summary_without_raw_result', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/sensitive-instance-id' }));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-instance-id</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-instance-id</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Name>query_task</Name><Status>Failed</Status></Task></Tasks></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, `<Instance><Tasks><Task><Status>Failed</Status><Result Transform="Base64">${b64('semantic error on sensitive_table')}</Result></Task></Tasks></Instance>`));

  assert.throws(
    () => gas.executeSqlQuery_('select id, name from t'),
    (err) => {
      assert.match(err.message, /SQL 执行失败: semantic error on sensitive_table/);
      assert.match(err.message, /instanceId=sensitive-instance-id/);
      return true;
    }
  );
});

test('execute_sql_timeout_and_abnormal_status_surface_raw_instance_id', () => {
  const timeoutGas = loadGasContext();
  timeoutGas.DEFAULT_SQL_TIMEOUT_SECONDS = 0;
  timeoutGas.MIN_SQL_TIMEOUT_SECONDS = 0;
  timeoutGas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/sensitive-timeout-id' }));
  timeoutGas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-timeout-id</Name><Status>Running</Status></Instance>'));

  assert.throws(
    () => timeoutGas.executeSqlQuery_('select id from t'),
    (err) => {
      assert.match(err.message, /SQL 执行超时/);
      assert.match(err.message, /instanceId=sensitive-timeout-id/);
      return true;
    }
  );

  const abnormalGas = loadGasContext();
  abnormalGas.__urlFetchQueue.push(makeHttpResponse(201, '', { Location: 'https://service/projects/proj/instances/sensitive-abnormal-id' }));
  abnormalGas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-abnormal-id</Name><Status>Terminated</Status></Instance>'));
  abnormalGas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>sensitive-abnormal-id</Name><Status>Terminated</Status></Instance>'));
  abnormalGas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Name>query_task</Name><Status>Cancelled</Status></Task></Tasks></Instance>'));

  assert.throws(
    () => abnormalGas.executeSqlQuery_('select id from t'),
    (err) => {
      assert.match(err.message, /SQL 任务状态异常: Cancelled/);
      assert.match(err.message, /instanceId=sensitive-abnormal-id/);
      return true;
    }
  );
});

test('get_job_progress_running_returns_non_terminal', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>inst-1</Name><Status>Running</Status></Instance>'));
  assert.deepEqual(plain(gas.getJobProgress_('inst-1')), {
    instanceTerminated: false,
    instanceStatus: 'Running'
  });
  assert.equal(gas.__urlFetchCalls.length, 1);
});

test('instance_id_validation_rejects_invalid_values_before_http_request', () => {
  const gas = loadGasContext();
  const invalid = [
    '',
    '../inst',
    'inst/1',
    'inst?x=1',
    'x'.repeat(129)
  ];

  for (const instanceId of invalid) {
    assert.throws(() => gas.getQueryProgress(instanceId), /instanceId 不能为空|Instance ID 格式不正确/);
  }
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('get_job_progress_failed_returns_raw_error_summary', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>inst-1</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Name>query_task</Name><Status>Failed</Status></Task></Tasks></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, `<Instance><Tasks><Task><Status>Failed</Status><Result Transform="Base64">${b64('semantic error on sensitive_table')}</Result></Task></Tasks></Instance>`));
  const progress = gas.getJobProgress_('inst-1');
  assert.equal(progress.instanceTerminated, true);
  assert.equal(progress.taskStatus, 'Failed');
  assert.equal(progress.errorSummary, 'semantic error on sensitive_table');
  assert.equal(Object.hasOwn(progress, 'errorMessage'), false);
});

test('get_job_progress_failed_xml_error_returns_raw_code_and_message', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Name>inst-1</Name><Status>Terminated</Status></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Name>query_task</Name><Status>Failed</Status></Task></Tasks></Instance>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, `<Instance><Tasks><Task><Status>Failed</Status><Result Transform="Base64">${b64('<Error><Code>SemanticError</Code><Message>bad sensitive_table query</Message></Error>')}</Result></Task></Tasks></Instance>`));
  const progress = gas.getJobProgress_('inst-1');
  assert.equal(progress.errorSummary, 'SemanticError: bad sensitive_table query');
});

test('cancel_query_sends_put_terminate_instance_request', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Status>Terminated</Status></Instance>'));

  const result = gas.cancelQuery('inst-1');

  assert.equal(result.instanceId, 'inst-1');
  assert.equal(result.killResult, 'ok');
  assert.equal(gas.__urlFetchCalls.length, 1);
  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj\/instances\/inst-1\?curr_project=proj$/);
  assert.equal(gas.__urlFetchCalls[0].options.method, 'put');
  assert.match(gas.__urlFetchCalls[0].options.payload, /<Status>Terminated<\/Status>/);
});

test('cancel_query_logs_raw_instance_id_for_traceability', () => {
  const gas = loadGasContext({
    spreadsheetOptions: {
      name: 'Sensitive Workbook',
      id: 'spreadsheet-secret-id'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Status>Terminated</Status></Instance>'));

  gas.cancelQuery('sensitive-instance-id');

  const logText = gas.__logs.join('\n');
  assert.match(logText, /instanceId=sensitive-instance-id/);
  assert.match(logText, /spreadsheetName=Sensitive Workbook/);
  assert.match(logText, /spreadsheetId=spreadsheet-secret-id/);
});

test('cancel_query_returns_raw_exception_message', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(new Error('network failed for sensitive_project sensitive-instance-id'));

  const result = gas.cancelQuery('sensitive-instance-id');
  const text = JSON.stringify(result) + '\n' + gas.__logs.join('\n');

  assert.match(result.killResult, /^failed:exception:network failed for sensitive_project sensitive-instance-id$/);
  assert.match(text, /instanceId=sensitive-instance-id/);
  assert.match(text, /result=exception err=network failed for sensitive_project sensitive-instance-id/);
});

test('get_job_result_uses_curr_project', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));
  const result = gas.getJobResult_('inst-1');
  assert.equal(result.rowCount, 1);
  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj\/instances\/inst-1\?/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_project=proj/);
  assert.match(gas.__urlFetchCalls[0].url, /result/);
});

test('list_tables_uses_curr_schema_and_prefix', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Tables><Table><Name>abc_table</Name><Type>MANAGED_TABLE</Type><Comment>c</Comment></Table></Tables>'));
  const tables = gas.listTables_('s1', 'abc');
  assert.deepEqual(plain(tables), [{ name: 'abc_table', type: 'MANAGED_TABLE', comment: 'c' }]);
  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj\/tables\?/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_project=proj/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_schema=s1/);
  assert.match(gas.__urlFetchCalls[0].url, /prefix=abc/);
  assert.match(gas.__urlFetchCalls[0].url, /maxitems=1000/);
});

test('catalog_non_200_errors_include_raw_server_summary', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(403, '<Error><Code>Forbidden</Code><Message>denied on sensitive_schema</Message></Error>'));
  gas.__urlFetchQueue.push(makeHttpResponse(404, '<Error><Code>NoSuchObject</Code><Message>missing sensitive_table</Message></Error>'));
  gas.__urlFetchQueue.push(makeHttpResponse(400, '<Error><Code>BadRequest</Code><Message>bad secret_col</Message></Error>'));

  assert.throws(
    () => gas.listSchemas_(),
    (err) => {
      assert.match(err.message, /获取 Schema 列表失败 .*Forbidden: denied on sensitive_schema/);
      return true;
    }
  );
  assert.throws(
    () => gas.listTables_('sensitive_schema'),
    (err) => {
      assert.match(err.message, /获取表列表失败 .*NoSuchObject: missing sensitive_table/);
      return true;
    }
  );
  assert.throws(
    () => gas.getTableSchema_('sensitive_table', 'sensitive_schema'),
    (err) => {
      assert.match(err.message, /获取表结构失败 .*BadRequest: bad secret_col/);
      return true;
    }
  );
});

test('get_table_schema_uses_asynccache_and_curr_schema', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Name>t1</Name><Schema><Column><Name>id</Name><Type>bigint</Type></Column></Schema></Table>'));
  const schema = gas.getTableSchema_('t1', 's1');
  assert.deepEqual(plain(schema.columns), [{ name: 'id', type: 'bigint', comment: '', nullable: true }]);
  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj\/tables\/t1\?/);
  assert.match(gas.__urlFetchCalls[0].url, /asynccache/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_project=proj/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_schema=s1/);
});

test('catalog_path_segments_encode_slashes_in_project_and_table_names', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'sk',
      MC_PROJECT: 'proj/with space',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Name>table/a</Name><Schema></Schema></Table>'));

  gas.getTableSchema_('table/a', 's1');

  assert.match(gas.__urlFetchCalls[0].url, /\/projects\/proj%2Fwith%20space\/tables\/table%2Fa\?/);
  assert.match(gas.__urlFetchCalls[0].url, /curr_project=proj%2Fwith%20space/);
});

test('list_partitions_non_200_throws_error', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(403, '<Error><Code>Forbidden</Code><Message>denied on sensitive_table</Message></Error>'));
  assert.throws(
    () => gas.listPartitions_('t1', 's1'),
    (err) => {
      assert.match(err.message, /获取分区列表失败 .*Forbidden: denied on sensitive_table/);
      return true;
    }
  );
});

test('write_result_to_sheet_writes_header_and_rows', () => {
  const gas = loadGasContext();
  gas.writeResultToSheet_({
    columns: ['id', 'name'],
    rows: [['1', 'ok'], ['2', 'done']],
    row_count: 2
  }, 'Result');

  const sheet = gas.__spreadsheet.getSheetByName('Result');
  assert.deepEqual(sheet.values[0], ['id', 'name']);
  assert.deepEqual(sheet.values[1], ['1', 'ok']);
  assert.deepEqual(sheet.values[2], ['2', 'done']);
  assert.ok(sheet.calls.some((call) => call[0] === 'setFrozenRows' && call[1] === 1));
  assert.ok(sheet.calls.some((call) => call[0] === 'activate'));
});

test('write_result_to_sheet_logs_raw_sheet_name_for_traceability', () => {
  const gas = loadGasContext();
  gas.writeResultToSheet_({
    columns: ['id'],
    rows: [['1']],
    row_count: 1
  }, 'Sensitive Output');

  const logText = gas.__logs.join('\n');
  assert.match(logText, /sheetName=Sensitive Output/);
});

test('prepare_result_data_normalizes_rows_to_match_column_count', () => {
  const gas = loadGasContext();
  const data = gas.prepareResultData_({
    columns: ['id', '', 'status'],
    rows: [['1'], ['2', null, 'ok', 'extra']],
    rowCount: 2
  }, null);

  assert.deepEqual(plain(data.columns), ['id', 'Column 2', 'status']);
  assert.deepEqual(plain(data.rows), [
    ['1', '', ''],
    ['2', '', 'ok']
  ]);
});

test('prepare_result_data_escapes_formula_like_sheet_values', () => {
  const gas = loadGasContext();
  const data = gas.prepareResultData_({
    columns: ['=Header', 'name'],
    rows: [
      ['=IMPORTDATA("https://example.test")', '+plus'],
      ['-minus', '@user'],
      [' normal', '\tTabbed'],
      [42, true]
    ],
    rowCount: 4
  }, null);

  assert.deepEqual(plain(data.columns), ["'=Header", 'name']);
  assert.deepEqual(plain(data.rows), [
    ['\'=IMPORTDATA("https://example.test")', "'+plus"],
    ["'-minus", "'@user"],
    [' normal', "'\tTabbed"],
    [42, true]
  ]);
});

test('write_result_to_sheet_normalizes_invalid_target_sheet_name', () => {
  const gas = loadGasContext();
  const sheetName = 'Bad:/Name?With*Invalid[Chars]'.repeat(5);

  gas.writeResultToSheet_({
    columns: ['id'],
    rows: [['1']],
    row_count: 1
  }, sheetName);

  const created = Array.from(gas.__spreadsheet.sheets.keys())
    .find((name) => name.startsWith('Bad__Name_With_Invalid_Chars_'));
  assert.equal(typeof created, 'string');
  assert.equal(created.length <= 100, true);
  assert.doesNotMatch(created, /[\[\]\*\?\/\\:]/);
});

test('write_query_result_writes_empty_result_status_instead_of_throwing', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Status>Success</Status></Task></Tasks></Instance>'));

  const summary = gas.writeQueryResult('inst-empty', 'Empty Result');
  assert.equal(summary.emptyResult, true);
  assert.equal(summary.rowCount, 0);
  assert.equal(summary.columnCount, 0);

  const sheet = gas.__spreadsheet.getSheetByName('Empty Result');
  assert.deepEqual(sheet.values[0], ['Status']);
  assert.match(sheet.values[1][0], /No tabular result/);
});

test('write_query_result_parse_failures_do_not_create_sheet_or_expose_raw_result_context', () => {
  const gas = loadGasContext();
  gas.Utilities.base64Decode = () => {
    throw new Error('bad base64 near sensitive_table secret_col');
  };
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance><Tasks><Task><Status>Success</Status><Result Transform="Base64">not-base64</Result></Task></Tasks></Instance>'));

  assert.throws(
    () => gas.writeQueryResult('inst-parse-fail', 'Parse Failure Result'),
    (err) => {
      assert.match(err.message, /解析结果失败: bad base64 near sensitive_table secret_col/);
      return true;
    }
  );
  assert.equal(gas.__spreadsheet.getSheetByName('Parse Failure Result'), null);
});

test('write_query_result_respects_requested_row_limit', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n2,done\n3,skip\n')));

  const summary = gas.writeQueryResult('inst-limited', 'Limited Result', 2);
  assert.equal(summary.rowCount, 2);
  assert.equal(summary.totalRowCount, 3);
  assert.equal(summary.truncated, true);

  const sheet = gas.__spreadsheet.getSheetByName('Limited Result');
  assert.deepEqual(sheet.values[0], ['id', 'name']);
  assert.deepEqual(sheet.values[1], ['1', 'ok']);
  assert.deepEqual(sheet.values[2], ['2', 'done']);
  assert.equal(sheet.values[3], undefined);
});

test('write_query_result_uses_document_lock', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  gas.writeQueryResult('inst-1', 'Locked Result');

  assert.deepEqual(gas.__lockCalls, [
    ['getDocumentLock'],
    ['tryLock', 30000],
    ['releaseLock']
  ]);
});

test('write_query_result_fails_when_document_lock_is_unavailable', () => {
  const gas = loadGasContext({ lockAvailable: false });
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id,name\n1,ok\n')));

  assert.throws(() => gas.writeQueryResult('inst-1', 'Locked Result'), /正在写入查询结果/);
  assert.deepEqual(gas.__lockCalls, [
    ['getDocumentLock'],
    ['tryLock', 30000]
  ]);
  assert.equal(gas.__spreadsheet.getSheetByName('Locked Result'), null);
});

test('prepare_result_data_caps_rows_at_10000', () => {
  const gas = loadGasContext();
  const rows = Array.from({ length: 10005 }, (_, i) => [String(i)]);
  const data = gas.prepareResultData_({
    columns: ['id'],
    rows,
    rowCount: rows.length
  }, null);

  assert.equal(data.rows.length, 10000);
  assert.equal(data.row_count, 10000);
  assert.equal(data.total_row_count, 10005);
  assert.equal(data.truncated, true);
});

test('run_local_safety_smoke_tests_returns_structured_summary_without_http', () => {
  const gas = loadGasContext();

  const summary = gas.runLocalSafetySmokeTests();

  assert.equal(summary.failed, 0);
  assert.equal(summary.passed, 2);
  assert.equal(summary.skipped, 1);
  assert.deepEqual(Array.from(summary.results, (r) => r.name), [
    'read_only_sql_guard',
    'endpoint_validation',
    'real_service_checks'
  ]);
  assert.equal(gas.__urlFetchCalls.length, 0);
});

test('release_smoke_summary_completes_with_catalog_responses', () => {
  const gas = loadGasContext({
    userProperties: {
      ALIYUN_ACCESS_KEY_ID: 'ak',
      ALIYUN_ACCESS_KEY_SECRET: 'sk',
      MC_PROJECT: 'sensitive_project_name',
      MC_ENDPOINT: 'https://service.ap-southeast-1.maxcompute.aliyun.com/api'
    }
  });
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas><Schema><Name>sensitive_schema</Name></Schema></Schemas>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas><Schema><Name>sensitive_schema</Name></Schema></Schemas>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Tables><Table><Name>sensitive_table</Name><Type>MANAGED_TABLE</Type></Table></Tables>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Name>sensitive_table</Name><Schema><Column><Name>secret_col</Name><Type>bigint</Type></Column></Schema></Table>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, resultXml('id\n1\n')));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Instance></Instance>'));

  const summary = gas.runReleaseSmokeTests();

  assert.equal(summary.failed, 0);
});

test('catalog_parse_failure_logs_raw_parser_message_for_traceability', () => {
  const gas = loadGasContext();
  gas.XmlService.parse = () => {
    throw new Error('parse failed near sensitive_schema sensitive_table secret_col');
  };

  const schemas = gas.parseSchemasXml_('<Schemas><Schema><Name>sensitive_schema</Name></Schema></Schemas>');

  assert.deepEqual(plain(schemas), []);
  const text = gas.__logs.join('\n');
  assert.match(text, /解析 Schema 列表 XML 失败: parse failed near sensitive_schema sensitive_table secret_col/);
});

test('catalog_entrypoints_surface_raw_parse_errors_instead_of_empty_results', () => {
  const gas = loadGasContext();
  gas.XmlService.parse = () => {
    throw new Error('parse failed near sensitive_schema sensitive_table secret_col');
  };

  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Schemas><Schema><Name>sensitive_schema</Name></Schema></Schemas>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Tables><Table><Name>sensitive_table</Name></Table></Tables>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Name>sensitive_table</Name><Schema></Schema></Table>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Partition><Name>pt=sensitive_partition</Name></Partition></Table>'));

  for (const [prefix, fn] of [
    ['解析 Schema 列表 XML 失败', () => gas.listSchemas_()],
    ['解析表列表 XML 失败', () => gas.listTables_('sensitive_schema')],
    ['解析表结构 XML 失败', () => gas.getTableSchema_('sensitive_table', 'sensitive_schema')],
    ['解析分区列表 XML 失败', () => gas.listPartitions_('sensitive_table', 'sensitive_schema')]
  ]) {
    assert.throws(
      fn,
      (err) => {
        assert.match(err.message, new RegExp(`${prefix}: parse failed near sensitive_schema sensitive_table secret_col`));
        return true;
      }
    );
  }
});

test('catalog_entrypoints_reject_unexpected_xml_roots_with_raw_message', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<SensitiveSchemas><Schema><Name>sensitive_schema</Name></Schema></SensitiveSchemas>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<SensitiveTables><Table><Name>sensitive_table</Name></Table></SensitiveTables>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<SensitiveTable><Name>sensitive_table</Name></SensitiveTable>'));
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<SensitivePartitions><Partition><Name>pt=sensitive_partition</Name></Partition></SensitivePartitions>'));

  for (const [prefix, fn] of [
    ['解析 Schema 列表 XML 失败', () => gas.listSchemas_()],
    ['解析表列表 XML 失败', () => gas.listTables_('sensitive_schema')],
    ['解析表结构 XML 失败', () => gas.getTableSchema_('sensitive_table', 'sensitive_schema')],
    ['解析分区列表 XML 失败', () => gas.listPartitions_('sensitive_table', 'sensitive_schema')]
  ]) {
    assert.throws(
      fn,
      (err) => {
        assert.match(err.message, new RegExp(`${prefix}: Unexpected XML root`));
        return true;
      }
    );
  }
});

test('catalog_entrypoints_reject_unexpected_json_schema_shape_with_raw_message', () => {
  const gas = loadGasContext();
  gas.__urlFetchQueue.push(makeHttpResponse(200, '<Table><Name>sensitive_table</Name><Schema format="Json">{"error":"sensitive_schema secret_col"}</Schema></Table>'));

  assert.throws(
    () => gas.getTableSchema_('sensitive_table', 'sensitive_schema'),
    (err) => {
      assert.match(err.message, /解析表结构 XML 失败: Unexpected JSON schema shape/);
      return true;
    }
  );
});

test('run_release_smoke_tests_fails_clearly_when_connection_is_missing', () => {
  const gas = loadGasContext({
    scriptProperties: null
  });

  assert.throws(() => gas.runReleaseSmokeTests(), /Release smoke tests failed: connection_status/);
  assert.equal(gas.__urlFetchCalls.length, 0);
});


// ============================================================
// 查询历史（PropertiesService 跨设备同步）
// ============================================================

test('get_query_history_returns_empty_defaults_for_fresh_user', () => {
  const gas = loadGasContext();
  const result = plain(gas.getQueryHistory());
  assert.deepEqual(result.sqlItems, []);
  assert.deepEqual(result.instanceItems, []);
  assert.equal(result.enabled, true);
});

test('append_sql_history_persists_to_user_properties_and_dedupes', () => {
  const gas = loadGasContext();

  assert.deepEqual(plain(gas.appendSqlHistory('SELECT 1')).items, ['SELECT 1']);
  assert.deepEqual(plain(gas.appendSqlHistory('SELECT 2')).items, ['SELECT 2', 'SELECT 1']);
  assert.deepEqual(plain(gas.appendSqlHistory('SELECT 1')).items, ['SELECT 1', 'SELECT 2']);

  const stored = JSON.parse(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'));
  assert.deepEqual(stored, ['SELECT 1', 'SELECT 2']);
});

test('append_sql_history_caps_at_ten_entries', () => {
  const gas = loadGasContext();
  for (let i = 1; i <= 12; i++) {
    gas.appendSqlHistory('SELECT ' + i);
  }
  const stored = JSON.parse(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'));
  assert.equal(stored.length, 10);
  assert.equal(stored[0], 'SELECT 12');
  assert.equal(stored[9], 'SELECT 3');
});

test('append_sql_history_truncates_oversized_entries_before_storage', () => {
  const gas = loadGasContext();
  const long = 'SELECT ' + 'x'.repeat(8000);
  const result = plain(gas.appendSqlHistory(long));
  assert.equal(result.items.length, 1);
  assert.equal(result.items[0].length, 4 * 1024);
  assert.ok(result.items[0].startsWith('SELECT '));
});

test('append_sql_history_is_noop_when_disabled', () => {
  const gas = loadGasContext({
    userProperties: { MC_SQL_HISTORY_ENABLED: 'false' }
  });
  const result = plain(gas.appendSqlHistory('SELECT 1'));
  assert.deepEqual(result.items, []);
  assert.equal(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'), null);
});

test('append_sql_history_skips_blank_input_without_touching_storage', () => {
  const gas = loadGasContext();
  gas.appendSqlHistory('SELECT 1');
  const result = plain(gas.appendSqlHistory('   '));
  assert.deepEqual(result.items, ['SELECT 1']);
});

test('remove_sql_history_at_index_drops_single_entry', () => {
  const gas = loadGasContext();
  gas.appendSqlHistory('SELECT 1');
  gas.appendSqlHistory('SELECT 2');
  gas.appendSqlHistory('SELECT 3');

  const result = plain(gas.removeSqlHistoryAt(1));
  assert.deepEqual(result.items, ['SELECT 3', 'SELECT 1']);
  const stored = JSON.parse(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'));
  assert.deepEqual(stored, ['SELECT 3', 'SELECT 1']);
});

test('remove_sql_history_at_invalid_index_is_safe_noop', () => {
  const gas = loadGasContext();
  gas.appendSqlHistory('SELECT 1');
  const result = plain(gas.removeSqlHistoryAt(42));
  assert.deepEqual(result.items, ['SELECT 1']);
});

test('clear_sql_history_deletes_property_but_leaves_instance_history', () => {
  const gas = loadGasContext();
  gas.appendSqlHistory('SELECT 1');
  gas.appendInstanceHistory('inst-keepme');

  const result = plain(gas.clearSqlHistory());
  assert.deepEqual(result.items, []);
  assert.equal(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'), null);

  const stored = JSON.parse(gas.PropertiesService.getUserProperties().getProperty('MC_INSTANCE_HISTORY'));
  assert.equal(stored.length, 1);
  assert.equal(stored[0].instanceId, 'inst-keepme');
});

test('set_sql_history_enabled_false_wipes_sql_history_and_persists_flag', () => {
  const gas = loadGasContext();
  gas.appendSqlHistory('SELECT 1');

  const result = plain(gas.setSqlHistoryEnabled(false));
  assert.equal(result.enabled, false);
  assert.equal(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY_ENABLED'), 'false');
  assert.equal(gas.PropertiesService.getUserProperties().getProperty('MC_SQL_HISTORY'), null);

  // Re-enabling does not resurrect the old entries.
  const resumed = plain(gas.setSqlHistoryEnabled(true));
  assert.equal(resumed.enabled, true);
  assert.deepEqual(plain(gas.appendSqlHistory('SELECT 2')).items, ['SELECT 2']);
});

test('append_instance_history_rejects_invalid_instance_ids', () => {
  const gas = loadGasContext();
  const result = plain(gas.appendInstanceHistory('inst with spaces'));
  assert.deepEqual(result.items, []);
  assert.equal(gas.PropertiesService.getUserProperties().getProperty('MC_INSTANCE_HISTORY'), null);
});

test('get_query_history_prunes_instance_entries_older_than_one_day', () => {
  const day = 24 * 60 * 60 * 1000;
  const userProperties = {
    MC_INSTANCE_HISTORY: JSON.stringify([
      { instanceId: 'inst-fresh', savedAt: Date.now() - 60_000 },
      { instanceId: 'inst-expired', savedAt: Date.now() - 2 * day }
    ])
  };
  const gas = loadGasContext({ userProperties });

  const result = plain(gas.getQueryHistory());
  assert.equal(result.instanceItems.length, 1);
  assert.equal(result.instanceItems[0].instanceId, 'inst-fresh');
});

test('get_query_history_ignores_corrupt_property_payload', () => {
  const gas = loadGasContext({
    userProperties: {
      MC_SQL_HISTORY: '{not json',
      MC_INSTANCE_HISTORY: '<<corrupt>>'
    }
  });
  const result = plain(gas.getQueryHistory());
  assert.deepEqual(result.sqlItems, []);
  assert.deepEqual(result.instanceItems, []);
});
