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

import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.BucketPath;
import com.google.common.collect.Maps;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsToken;
import oracle.goldengate.datasource.adapt.Op;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RecordBuilder {
    private final static Logger logger = LoggerFactory
            .getLogger(RecordBuilder.class);

    private Configure configure;
    private Map<String, Integer> latestSyncId = Maps.newHashMap();
    private final static SimpleDateFormat DEFAULT_DATE_FORMATTER =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private Charset charset;

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

        if (!Charset.isSupported(configure.getCharsetName())) {
            throw new InvalidParameterException("Invalid charsetName: " + configure.getCharsetName());
        }
        charset = Charset.forName(configure.getCharsetName());

        for (String oracleTableFullName : configure.getTableMappings().keySet()) {
            latestSyncId.put(oracleTableFullName, 0);
        }
    }

    public RecordEntry buildRecord(Op op, String opType, TableMapping tableMapping) throws ParseException {
        RecordEntry recordEntry = new RecordEntry();

        logger.debug("BuildRecord, oracle table: {}, record: {}", tableMapping.getOracleFullTableName(), op.getRecord().toString());

        if (tableMapping.getRecordSchema() == null) {
            // blob topic
            buildBlobRecord(op, opType, tableMapping, recordEntry);
        } else {
            buildTupleRecord(op, opType, tableMapping, recordEntry);
        }

        Integer syncId = this.latestSyncId.get(tableMapping.getOracleFullTableName());
        String strSyncId = String.format("%06d", syncId);

        recordEntry.addAttribute(Constant.SYNCID, strSyncId);
        recordEntry.addAttribute(Constant.VERSION, "1.0");
        recordEntry.addAttribute(Constant.SRC_TYPE, "Oracle");
        recordEntry.addAttribute(Constant.SRC_ID, configure.getOracleSid());
        recordEntry.addAttribute(Constant.TS, op.getTimestamp());
        recordEntry.addAttribute(Constant.DBNAME, tableMapping.getOracleSchema());
        recordEntry.addAttribute(Constant.TABNMAE, tableMapping.getOracleTableName());
        recordEntry.addAttribute(Constant.OPER_TYPE, opType);

        this.latestSyncId.put(tableMapping.getOracleFullTableName(), (syncId++) % 1000000);

        return recordEntry;
    }

    private void buildTupleRecord(Op op, String opType, TableMapping tableMapping, RecordEntry recordEntry) throws ParseException {
        TupleRecordData recordData = new TupleRecordData(tableMapping.getRecordSchema());
        RecordSchema recordSchema = tableMapping.getRecordSchema();
        StringBuilder hashString = new StringBuilder();

        List<DsColumn> columns = op.getColumns();

        String rowIdColumn = tableMapping.getRowIdColumn();
        if (StringUtils.isNotBlank(rowIdColumn)) {
            DsToken token = op.getRecord().getUserToken(Constant.ROWID_TOKEN);
            if (!token.isSet()) {
                logger.error("BuildRecord failed, oracle table token TKN-ROWID is not set, can not get oracle rowid, table: {}",
                        tableMapping.getOracleFullTableName());
                throw new RuntimeException("oracle table token TKN-ROWID is not set, can not get oracle rowid");
            }
            recordData.setField(rowIdColumn, token.getValue());
        }

        String ctype = tableMapping.getcTypeColumn();
        if (StringUtils.isNotBlank(ctype)) {
            recordData.setField(ctype, opType);
        }

        String ctime = tableMapping.getcTimeColumn();
        if (StringUtils.isNotBlank(ctime)) {
            if (recordSchema.getField(ctime).getType() == FieldType.STRING) {
                recordData.setField(ctime, op.getTimestamp());
            } else if (recordSchema.getField(ctime).getType() == FieldType.TIMESTAMP) {
                recordData.setField(ctime, convertStrToMicroseconds(op.getTimestamp()));
            } else {
                logger.error("BuildRecord failed, cTimeColumn type must be string or timestamp in DataHub, type: {}",
                        recordSchema.getField(ctime).getType().name());
                throw new RuntimeException("cTimeColumn type must be string or timestamp in DataHub");
            }
        }

        String cId = tableMapping.getcIdColumn();
        if (StringUtils.isNotBlank(cId)) {
            recordData.setField(cId, Long.toString(HandlerInfoManager.instance().getRecordId()));
        }


        Date readTime = DEFAULT_DATE_FORMATTER.parse(op.getTimestamp());
        Map<String, String> constMap = tableMapping.getConstColumnMappings();
        if (constMap != null && !constMap.isEmpty()) {
            for (Map.Entry<String, String> entry : constMap.entrySet()) {
                recordData.setField(entry.getKey(), BucketPath.escapeString(entry.getValue(),
                        readTime.getTime(), tableMapping.getConstColumnMappings()));
            }
        }

        for (int i = 0; i < columns.size(); i++) {

            String columnName = op.getTableMeta().getColumnName(i).toLowerCase();
            ColumnMapping columnMapping = tableMapping.getColumnMappings().get(columnName);
            if (columnMapping == null) {
                logger.debug("BuildRecord, oracle table column is not configured, table: {}, column: {}",
                        op.getTableMeta().getTableName().getFullName(), columnName);
                continue;
            }

            DsColumn dsColumn = columns.get(i);
            String afterValue = dsColumn.getAfterValue();
            String beforeValue = dsColumn.getBeforeValue();
            if (!columnMapping.isDefaultCharset()) {
                afterValue = dsColumn.getAfter() == null ? null : new String(dsColumn.getAfterValue().getBytes(charset));
                beforeValue = dsColumn.getBefore() == null ? null : new String(dsColumn.getBeforeValue().getBytes(charset));
            }
            logger.info("after {}, before {}", afterValue, beforeValue);

            String dest = columnMapping.getDest();
            if (StringUtils.isNotBlank(dest)) {
                if (columnMapping.isKeyColumn()) {
                    if (dsColumn.getAfter() == null) {
                        setTupleData(recordData, recordSchema.getField(dest), beforeValue,
                                columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                    } else {
                        setTupleData(recordData, recordSchema.getField(dest), afterValue,
                                columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                    }
                } else {
                    setTupleData(recordData, recordSchema.getField(dest), afterValue,
                            columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                }
            }

            String destOld = columnMapping.getDestOld();
            if (StringUtils.isNotBlank(destOld)) {
                setTupleData(recordData, recordSchema.getField(destOld), beforeValue,
                        columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
            }

            if (columnMapping.isShardColumn()) {
                hashString.append(afterValue);
            }
        }

        recordEntry.setPartitionKey(hashString.toString());
        recordEntry.setRecordData(recordData);
    }

    private void buildBlobRecord(Op op, String opType, TableMapping tableMapping, RecordEntry recordEntry) {
        List<DsColumn> columns = op.getColumns();
        if (tableMapping.getColumnMappings().size() != 1) {
            logger.error("BuildRecord failed, oracle table must have only one column for blob topic, " +
                            "oracle table: {}, DataHub topic: {}, column num: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), op.getNumColumns());
            throw new RuntimeException("oracle table must have only one column for blob topic");
        }

        for (int i = 0; i < columns.size(); i++) {

            String columnName = op.getTableMeta().getColumnName(i).toLowerCase();
            ColumnMapping columnMapping = tableMapping.getColumnMappings().get(columnName);
            if (columnMapping == null) {
                continue;
            }

            DsColumn dsColumn = columns.get(i);
            byte[] data;
            if (columnMapping.isDefaultCharset()) {
                if ("D".equalsIgnoreCase(opType)) {
                    data = dsColumn.getBeforeValue().getBytes(Charset.forName("UTF-8"));
                } else {
                    data = dsColumn.getAfterValue().getBytes(Charset.forName("UTF-8"));
                }
            } else {
                if ("D".equalsIgnoreCase(opType)) {
                    data = dsColumn.getBeforeValue().getBytes(charset);
                } else {
                    data = dsColumn.getAfterValue().getBytes(charset);
                }
            }

            BlobRecordData recordData = new BlobRecordData(data);
            recordEntry.setRecordData(recordData);
            break;
        }

        recordEntry.addAttribute("opType", opType);
    }

    private void setTupleData(TupleRecordData recordData, Field field, String val, boolean isDateFormat, SimpleDateFormat format) {
        if (val == null || val.isEmpty() || field == null || "null".equalsIgnoreCase(val)) {
            return;
        }
        switch (field.getType()) {
            case STRING:
                recordData.setField(field.getName(), val);
                break;
            case BIGINT:
                recordData.setField(field.getName(), Long.parseLong(val));
                break;
            case DOUBLE:
                recordData.setField(field.getName(), Double.parseDouble(val));
                break;
            case BOOLEAN:
                recordData.setField(field.getName(), Boolean.parseBoolean(val));
                break;
            case TIMESTAMP:
                if (isDateFormat) {
                    if (format == null) {
                        // set timestamp Microseconds
                        recordData.setField(field.getName(), convertStrToMicroseconds(val));
                    } else {
                        try {
                            recordData.setField(field.getName(), format.parse(val).getTime() * 1000);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }


                } else {
                    recordData.setField(field.getName(), Long.parseLong(val));
                }
                break;
            case DECIMAL:
                recordData.setField(field.getName(), new BigDecimal(val));
                break;
            default:
                logger.error("BuildRecord failed, unknown DataHub filed type, type: {}", field.getType().name());
                throw new RuntimeException("unknown DataHub filed type " + field.getType().name());
        }
    }

    // convert time string like yyyy-mm-dd:hh:mm:ss[.fffffffff] to microseconds
    private Object convertStrToMicroseconds(String timeStr) {
        if (StringUtils.isBlank(timeStr)) {
            return null;
        }

        // convert yyyy-mm-dd:hh:mm:ss to yyyy-mm-dd hh:mm:ss
        StringBuilder sb = new StringBuilder(timeStr);
        if (sb.length() < 11) {
            logger.error("BuildRecord failed, convert timeStr to timestamp failed, invalid timeStr, timeStr: {}", timeStr);
            throw new RuntimeException("convert timeStr to timestamp failed, invalid timeStr, timeStr: " + timeStr);
        }
        sb.setCharAt(10, ' ');
        timeStr = sb.toString();

        Timestamp ts = java.sql.Timestamp.valueOf(timeStr);
        long milliseconds = ts.getTime();
        int nanos = ts.getNanos();
        return milliseconds * 1000 + nanos % 1000000 / 1000;
    }
}
