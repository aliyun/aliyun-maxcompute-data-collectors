/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.odps;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.*;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class OdpsRecordBuilder {
  public static final Log LOG
      = LogFactory.getLog(OdpsRecordBuilder.class.getName());

  private Column[] odpsColumns;
  private SimpleDateFormat dateFormat;
  private Map<String, OdpsType> colNameTypeMap;

  final static Set trueString = new HashSet() {{
    add("true");
    add("1");
    add("y");
  }};

  final static Set falseString = new HashSet() {{
    add("false");
    add("0");
    add("n");
  }};

  public OdpsRecordBuilder(Table odpsTable, String dateFormatString,
                           List<String> inputColNames) {
    TableSchema tableSchema = odpsTable.getSchema();
    odpsColumns = tableSchema.getColumns().toArray(new Column[0]);
    if (dateFormatString != null) {
      dateFormat = new SimpleDateFormat(dateFormatString);
    }
    colNameTypeMap = buildColNameTypeMap(inputColNames, tableSchema);
  }

  private Map<String, OdpsType> buildColNameTypeMap(List<String> inputColNames,
                                                    TableSchema tableSchema) {
    Map<String, OdpsType> odpsNameTypeMap = Maps.newHashMap();
    for (Column column : tableSchema.getColumns()) {
      odpsNameTypeMap.put(column.getName().toLowerCase(), column.getType());
    }
    Map<String, OdpsType> colNameTypeMap = Maps.newHashMap();
    for (String colName : inputColNames) {
      if (!StringUtils.isEmpty(colName)) {
        if (odpsNameTypeMap.containsKey(colName.toLowerCase())) {
          colNameTypeMap.put(colName.toLowerCase(),
                  odpsNameTypeMap.get(colName.toLowerCase()));
        } else {
          throw new RuntimeException(this.getClass().getName() +
              " buildColNameTypeMap() error, " +
              "field not exists in odps table, field=" + colName);
        }
      }
    }
    return colNameTypeMap;
  }

  public Record buildRecord(Map<String, Object> rowMap)
          throws ParseException {
    ArrayRecord record = new ArrayRecord(odpsColumns);
    for (Map.Entry<String, Object> mapEntry : rowMap.entrySet()) {
      try {
        String key = mapEntry.getKey();
        Object value = mapEntry.getValue();
        setField(record, key.toLowerCase(), value.toString(),
            colNameTypeMap.get(key.toLowerCase()));
      } catch (Exception e) {
        // If build record field failed, warn and skip it
//        LOG.warn("Input key (or value) is null, skip this field.", e);
//        LOG.warn("Input entry: " + mapEntry.toString());
//        LOG.warn("Input rowMap: " + Arrays.toString(rowMap.entrySet().toArray()));
      }
    }
    return record;
  }

  private void setField(ArrayRecord record, String field, String fieldValue,
                        OdpsType odpsType) throws ParseException {
    if (StringUtils.isNotEmpty(field)
            && StringUtils.isNotEmpty(fieldValue)) {
      switch (odpsType) {
        case STRING:
          record.setString(field, fieldValue);
          break;
        case BIGINT:
          record.setBigint(field, Long.parseLong(fieldValue));
          break;
        case DATETIME:
            record.setDatetime(field, dateFormat.parse(fieldValue));
          break;
        case DOUBLE:
          record.setDouble(field, Double.parseDouble(fieldValue));
          break;
        case BOOLEAN:
          if (trueString.contains(fieldValue.toLowerCase())) {
            record.setBoolean(field, true);
          } else if (falseString.contains(fieldValue.toLowerCase())) {
            record.setBoolean(field, false);
          }
          break;
        case DECIMAL:
          record.setDecimal(field, new BigDecimal(fieldValue));
          break;
        case CHAR:
          record.setChar(field, new Char(fieldValue));
          break;
        case VARCHAR:
          record.setVarchar(field, new Varchar(fieldValue));
          break;
        case TINYINT:
          record.setTinyint(field, Byte.parseByte(fieldValue));
          break;
        case SMALLINT:
          record.setSmallint(field, Short.parseShort(fieldValue));
          break;
        case INT:
          record.setInt(field, Integer.parseInt(fieldValue));
          break;
        case FLOAT:
          record.setFloat(field, Float.parseFloat(fieldValue));
          break;
        case DATE:
          record.setDate(field, new java.sql.Date(dateFormat.parse(fieldValue).getTime()));
          break;
        case TIMESTAMP:
          record.setTimestamp(field, new Timestamp(dateFormat.parse(fieldValue).getTime()));
          break;
        case BINARY:
          record.setBinary(field, new Binary(fieldValue.getBytes()));
          break;
        default:
          throw new RuntimeException("Unknown column type: " + odpsType);
      }
    }

  }
}
