/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.BucketPath;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecordBuilder {
    private final static Logger logger = LoggerFactory
            .getLogger(RecordBuilder.class);

    private Configure configure;
    private Map<String, Integer> latestSyncId = Maps.newHashMap();
    private final static SimpleDateFormat DEFAULT_DATE_FORMATTER =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");

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

    private static RecordBuilder recordBuilder;

    public static RecordBuilder instance() {
        return recordBuilder;
    }

    public static void init(Configure configure) {
        if (recordBuilder == null) {
            recordBuilder = new RecordBuilder(configure);
        }
    }

    private RecordBuilder(Configure configure) {
        this.configure = configure;

        for (String oracleTableFullName : configure.getTableMappings().keySet()) {
            latestSyncId.put(oracleTableFullName, 0);
        }
    }

    public void setFieldValue(RecordEntry recordEntry,
                              Field field,
                              boolean isValueNull,
                              String fieldValue,
                              boolean isDateFormat,
                              SimpleDateFormat simpleDateFormat) throws ParseException {
        if (field != null && !isValueNull) {
            switch (field.getType()) {
                case STRING:
                    recordEntry.setString(field.getName(), fieldValue);
                    break;
                case BIGINT:
                    recordEntry.setBigint(field.getName(), Long.parseLong(fieldValue));
                    break;
                case DOUBLE:
                    recordEntry.setDouble(field.getName(), Double.parseDouble(fieldValue));
                    break;
                case BOOLEAN:
                    if (trueString.contains(fieldValue.toLowerCase())) {
                        recordEntry.setBoolean(field.getName(), true);
                    } else if (falseString.contains(fieldValue.toLowerCase())) {
                        recordEntry.setBoolean(field.getName(), false);
                    }
                    break;
                case TIMESTAMP:
                    if (isDateFormat) {
                        Date date = simpleDateFormat.parse(fieldValue);
                        recordEntry.setTimeStamp(field.getName(), date.getTime());
                    } else {
                        recordEntry.setTimeStamp(field.getName(), Long.parseLong(fieldValue));
                    }

                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + field.getType() + " ,value is: " + fieldValue);
            }
        }
    }

    public RecordEntry buildRecord(Op op,
                                   String opType,
                                   TableMapping tableMapping) throws ParseException {
        RecordEntry recordEntry = new RecordEntry(tableMapping.getTopic().getRecordSchema());

        List<DsColumn> columns = op.getColumns();

        if (logger.isDebugEnabled()) {
            logger.debug("table:[" + tableMapping.getOracleFullTableName() + "] record:" + op.getRecord().toString());
        }

        String hashString = "";
        for (int i = 0; i < columns.size(); i++) {
            String columnName = op.getTableMeta().getColumnName(i).toLowerCase();
            ColumnMapping columnMapping = tableMapping.getColumnMappings().get(columnName);
            if (columnMapping == null) {
                logger.debug("column name : " + columnName + " is not configured.  the table name is :"  + op.getTableMeta().getTableName());
                throw new RuntimeException("column name : " + columnName + " is not configured.  the table name is :"  + op.getTableMeta().getTableName());
            }
            Field field = columnMapping.getField();

            if (field != null) {
                this.setFieldValue(recordEntry,
                        field,
                        columns.get(i).getAfter() == null || columns.get(i).getAfter().isValueNull(),
                        columns.get(i).getAfterValue(),
                        columnMapping.isDateFormat(),
                        columnMapping.getSimpleDateFormat());
            }

            Field oldField = columnMapping.getOldFiled();
            if (oldField != null && columns.get(i).hasBeforeValue()) {
                this.setFieldValue(recordEntry,
                        oldField,
                        columns.get(i).getBefore() == null || columns.get(i).getBefore().isValueNull(),
                        columns.get(i).getBeforeValue(),
                        columnMapping.isDateFormat(),
                        columnMapping.getSimpleDateFormat());
            }
            if (columnMapping.isShardColumn()) {
                hashString += columns.get(i).getAfterValue();
            }
        }

        // setFieldValue中有对field为null的判断
        // string写入变更类型
        this.setFieldValue(recordEntry, tableMapping.getCtypeField(),
            false, opType, false, null);

        // 毫秒的string写入变更时间
        this.setFieldValue(recordEntry, tableMapping.getCtimeField(),
            false, op.getTimestamp(), false, null);

        // 写入变更序号
        this.setFieldValue(recordEntry, tableMapping.getCidField(), false,
            Long.toString(HandlerInfoManager.instance().getRecordId()), false, null);

        // 写入const columns
        Date readTime = DEFAULT_DATE_FORMATTER.parse(op.getTimestamp());
        for (Map.Entry<String, String> e : tableMapping.getConstColumnMappings().entrySet()) {
            this.setFieldValue(recordEntry, tableMapping.getConstFieldMappings().get(e.getKey()), false,
                BucketPath.escapeString(e.getValue(), readTime.getTime(), tableMapping.getConstColumnMappings()), false, null);
        }

        Integer syncId = this.latestSyncId.get(tableMapping.getOracleFullTableName());

        String strSyncId = String.format("%06d", syncId);

        recordEntry.putAttribute(Constant.SYNCID, strSyncId);
        recordEntry.putAttribute(Constant.VERSION, "1.0");
        recordEntry.putAttribute(Constant.SRC_TYPE, "Oracle");
        recordEntry.putAttribute(Constant.SRC_ID, configure.getSid());
        recordEntry.putAttribute(Constant.TS, op.getTimestamp());
        recordEntry.putAttribute(Constant.DBNAME, tableMapping.getOracleSchema());
        recordEntry.putAttribute(Constant.TABNMAE, tableMapping.getOracleTableName());
        recordEntry.putAttribute(Constant.OPER_TYPE, opType);

        if (tableMapping.isShardHash()) {
            // for hashCode() may return negative values, should & 0x7fffffff to insure a positive value
            recordEntry.putAttribute(Constant.HASH, String.valueOf(hashString.hashCode() & Integer.MAX_VALUE));
        }
        this.latestSyncId.put(tableMapping.getOracleFullTableName(), (syncId++) % 1000000);

        return recordEntry;
    }
}
