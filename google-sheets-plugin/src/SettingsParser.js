// Copyright 2024-2026 Alibaba Cloud. Licensed under the Apache License, Version 2.0.
// See LICENSE file in the project root for license information.

/**
 * SQL Settings 解析器
 *
 * 将用户 SQL 开头的 `SET key=value;` 语句转换为 SQLTask settings hints，
 * 并从真正提交给 MaxCompute SQL parser 的 Query 中移除这些 SET 语句。
 */

var SettingsParser_ = (function() {
  var SETTING_KEY_PATTERN = /^[A-Za-z_][A-Za-z0-9_.-]*$/;
  var RESERVED_AUDIT_SETTING_PATTERN = /^EXT_(?:PLATFORM_ID|NODE_ID|DAGTYPE|TASK_ID|NODE_NAME|NODE_ONDUTY)$/i;

  /**
   * 解析用户 SQL 中的前置 SET 语句。
   *
   * @param {string} sql
   * @return {{sql: string, settings: Object}}
   */
  function parse_(sql) {
    sql = String(sql || '');

    var parts = splitSqlStatementParts_(sql);
    var settings = {};
    var removedThrough = 0;
    var hasParsedSet = false;

    for (var i = 0; i < parts.length; i++) {
      var part = parts[i];
      var keyword = getFirstSqlKeyword_(part.raw);

      if (!keyword) {
        if (hasParsedSet) {
          removedThrough = part.separatorEnd;
        }
        continue;
      }

      if (keyword !== 'SET') {
        break;
      }

      var parsed = parseSetStatement_(part.raw);
      settings[parsed.name] = parsed.value;
      hasParsedSet = true;
      removedThrough = part.separatorEnd;
    }

    if (!hasParsedSet) {
      return {
        sql: sql.trim(),
        settings: {}
      };
    }

    return {
      sql: sql.substring(removedThrough).replace(/^\s+/, ''),
      settings: settings
    };
  }

  /**
   * 按顶层分号切分 SQL，并保留原始字符串范围。
   *
   * @param {string} sql
   * @return {Array<{raw: string, start: number, end: number, separatorEnd: number}>}
   */
  function splitSqlStatementParts_(sql) {
    var parts = [];
    var statementStart = 0;
    var quote = null;
    var lineComment = false;
    var blockComment = false;

    for (var i = 0; i < sql.length; i++) {
      var ch = sql.charAt(i);
      var next = i + 1 < sql.length ? sql.charAt(i + 1) : '';

      if (lineComment) {
        if (ch === '
' || ch === '') {
          lineComment = false;
        }
        continue;
      }

      if (blockComment) {
        if (ch === '*' && next === '/') {
          blockComment = false;
          i++;
        }
        continue;
      }

      if (quote) {
        if (ch === '\' && next) {
          i++;
        } else if (ch === quote) {
          if (next === quote) {
            i++;
          } else {
            quote = null;
          }
        }
        continue;
      }

      if (ch === '-' && next === '-') {
        lineComment = true;
        i++;
        continue;
      }

      if (ch === '/' && next === '*') {
        blockComment = true;
        i++;
        continue;
      }

      if (ch === '\'' || ch === '"' || ch === '`') {
        quote = ch;
        continue;
      }

      if (ch === ';') {
        addSqlStatementPart_(parts, sql, statementStart, i, i + 1);
        statementStart = i + 1;
      }
    }

    addSqlStatementPart_(parts, sql, statementStart, sql.length, sql.length);
    return parts;
  }

  function addSqlStatementPart_(parts, sql, start, end, separatorEnd) {
    parts.push({
      raw: sql.substring(start, end),
      start: start,
      end: end,
      separatorEnd: separatorEnd
    });
  }

  /**
   * 解析单条 SET key=value 语句。
   *
   * @param {string} statement
   * @return {{name: string, value: string}}
   */
  function parseSetStatement_(statement) {
    var clean = stripSqlComments_(statement).replace(/^\s+|\s+$/g, '');
    var match = clean.match(/^SET\s+([^=\s]+)\s*=\s*([\s\S]*)$/i);
    if (!match) {
      throw new Error('SET 语句格式不正确，请使用 SET key=value;');
    }

    var name = match[1].replace(/^\s+|\s+$/g, '');
    var value = match[2].replace(/^\s+|\s+$/g, '');
    if (!SETTING_KEY_PATTERN.test(name)) {
      throw new Error('SET 语句配置名格式不正确。');
    }
    if (RESERVED_AUDIT_SETTING_PATTERN.test(name)) {
      throw new Error('不允许手动设置插件保留的 EXT_* 审计字段。');
    }

    return {
      name: name,
      value: unquoteSqlSettingValue_(value)
    };
  }

  /**
   * 删除注释，保留字符串内容，便于解析 SET 赋值。
   *
   * @param {string} sql
   * @return {string}
   */
  function stripSqlComments_(sql) {
    var out = [];
    var quote = null;
    var lineComment = false;
    var blockComment = false;

    for (var i = 0; i < sql.length; i++) {
      var ch = sql.charAt(i);
      var next = i + 1 < sql.length ? sql.charAt(i + 1) : '';

      if (lineComment) {
        if (ch === '
' || ch === '') {
          lineComment = false;
          out.push(ch);
        }
        continue;
      }

      if (blockComment) {
        if (ch === '*' && next === '/') {
          blockComment = false;
          out.push(' ');
          i++;
        }
        continue;
      }

      if (quote) {
        out.push(ch);
        if (ch === '\' && next) {
          out.push(next);
          i++;
        } else if (ch === quote) {
          if (next === quote) {
            out.push(next);
            i++;
          } else {
            quote = null;
          }
        }
        continue;
      }

      if (ch === '-' && next === '-') {
        lineComment = true;
        out.push(' ');
        i++;
        continue;
      }

      if (ch === '/' && next === '*') {
        blockComment = true;
        out.push(' ');
        i++;
        continue;
      }

      if (ch === '\'' || ch === '"') {
        quote = ch;
        out.push(ch);
        continue;
      }

      out.push(ch);
    }

    return out.join('');
  }

  /**
   * 将 SET value 中完整包裹的 SQL 字符串字面量还原为 hints 字符串。
   *
   * @param {string} value
   * @return {string}
   */
  function unquoteSqlSettingValue_(value) {
    if (value.length < 2) {
      return value;
    }

    var quote = value.charAt(0);
    if ((quote !== '\'' && quote !== '"') || value.charAt(value.length - 1) !== quote) {
      return value;
    }

    var inner = value.substring(1, value.length - 1);
    var doubled = new RegExp(quote + quote, 'g');
    return inner
      .replace(doubled, quote)
      .replace(/\'/g, '\'')
      .replace(/\"/g, '"')
      .replace(/\\/g, '\');
  }

  return {
    parse: parse_
  };
}());
