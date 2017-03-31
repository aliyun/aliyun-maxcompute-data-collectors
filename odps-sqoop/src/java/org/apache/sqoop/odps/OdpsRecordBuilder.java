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

import maxcompute.data.collectors.common.maxcompute.*;
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
        RecordUtil.setFieldValue(record, key.toLowerCase(), value == null ? null : value.toString(),
            colNameTypeMap.get(key.toLowerCase()), dateFormat);
      } catch (Exception e) {
        // If build record field failed, warn and skip it
//        LOG.warn("Input key (or value) is null, skip this field.", e);
//        LOG.warn("Input entry: " + mapEntry.toString());
//        LOG.warn("Input rowMap: " + Arrays.toString(rowMap.entrySet().toArray()));
      }
    }
    return record;
  }

}
