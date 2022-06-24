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

import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.MetricHelper;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.BucketPath;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsToken;
import oracle.goldengate.datasource.adapt.Op;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TableRecordBuilder {
    private final static Logger logger = LoggerFactory.getLogger(TableRecordBuilder.class);

    private static final int WAIT_TASK_TIMEOUT_MS = 5000;

    private Configure configure;
    private LinkedBlockingQueue<Record> recordQueue;

    private TableMapping tableMapping;
    private volatile boolean stop = true;

    private ExecutorService executor;
    private volatile Future currentTask = null;
    private long lastSubmitTimeMs = System.currentTimeMillis();

    public TableRecordBuilder(Configure configure, TableMapping tableMapping, ExecutorService executor) {
        this.configure = configure;
        this.tableMapping = tableMapping;
        this.executor = executor;
        this.recordQueue = new LinkedBlockingQueue<>(configure.getBuildRecordQueueSize());
    }

    public void start() {
        stop = false;
    }

    public void stop() {
        stop = true;
    }

    public void syncExec() {
        // 1. 保证当前task执行完
        awaitTaskDone();

        // 2. 重新提交完整处理task，并且保证该task一定提交成功
        boolean ret = false;
        do {
            ret = submit();
        } while (!ret);

        // 3. 等待任务执行完
        awaitTaskDone();
    }

    public boolean addRecord(Op op, String opType, String recordId) {
        boolean ret = false;
        if (!stop) {
            try {
                ret = recordQueue.offer(new Record(opType, recordId, op), configure.getBuildRecordQueueTimeoutMs(), TimeUnit.MILLISECONDS);
                if (isReady()) {
                    submit();
                }
            } catch (InterruptedException e) {
                logger.warn("Add record failed, table: {}", tableMapping.getOracleFullTableName(), e);
            }
        } else {
            throw new RuntimeException("Record builder has stopped.");
        }
        return ret;
    }

    private boolean isReady() {
        return recordQueue.size() >= configure.getBuildBatchSize()
                || System.currentTimeMillis() - lastSubmitTimeMs >= configure.getBuildBatchTimeoutMs();
    }

    private boolean taskDone() {
        return currentTask == null || currentTask.isDone();
    }

    private boolean submit() {
        boolean ret = true;
        if (!recordQueue.isEmpty() && taskDone()) {
            BuildTask task = new BuildTask(tableMapping);
            try {
                currentTask = executor.submit(task);
                lastSubmitTimeMs = System.currentTimeMillis();
            } catch (RejectedExecutionException e) {
                logger.warn("Submit build task failed, table: {}", tableMapping.getOracleFullTableName(), e);
                ret = false;
            }
        }
        return ret;
    }

    private void awaitTaskDone() {
        while (!taskDone()) {
            try {
                currentTask.get(WAIT_TASK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Wait build task finished error, table: {}", tableMapping.getOracleFullTableName(), e);
            } catch (TimeoutException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Wait build task finished timeout, table: {}", tableMapping.getOracleFullTableName());
                }
            }
        }
    }

    private RecordEntry buildRecord(Record record) {
        long startTime = System.currentTimeMillis();
        RecordEntry recordEntry = new RecordEntry();

        if (tableMapping.getRecordSchema() == null) {
            buildBlobRecord(record, tableMapping, recordEntry);
        } else {
            buildTupleRecord(record, tableMapping, recordEntry);
        }

        recordEntry.addAttribute(Constant.VERSION, "1.0");
        recordEntry.addAttribute(Constant.SRC_TYPE, "Oracle");
        recordEntry.addAttribute(Constant.TS, record.op.getTimestamp());
        recordEntry.addAttribute(Constant.DBNAME, tableMapping.getOracleSchema());
        recordEntry.addAttribute(Constant.TABNMAE, tableMapping.getOracleTableName());

        if (configure.isReportMetric()) {
            MetricHelper.instance().addBuildTime(System.currentTimeMillis() - startTime);
        }
        return recordEntry;
    }

    private void buildTupleRecord(Record record, TableMapping tableMapping, RecordEntry recordEntry) {
        TupleRecordData recordData = new TupleRecordData(tableMapping.getRecordSchema());
        RecordSchema recordSchema = tableMapping.getRecordSchema();
        StringBuilder hashString = new StringBuilder();

        List<DsColumn> columns = record.op.getColumns();

        String rowIdColumn = tableMapping.getRowIdColumn();
        if (StringUtils.isNotBlank(rowIdColumn)) {
            DsToken token = record.rowIdToken;
            if (!token.isSet()) {
                logger.error("BuildRecord failed, oracle table token TKN-ROWID is not set, can not get oracle rowid, table: {}",
                        tableMapping.getOracleFullTableName());
                throw new RuntimeException("oracle table token TKN-ROWID is not set, can not get oracle rowid");
            }
            recordData.setField(rowIdColumn, token.getValue());
        }

        String ctype = tableMapping.getcTypeColumn();
        if (StringUtils.isNotBlank(ctype)) {
            recordData.setField(ctype, record.opType);
        }

        String ctime = tableMapping.getcTimeColumn();
        if (StringUtils.isNotBlank(ctime)) {
            if (recordSchema.getField(ctime).getType() == FieldType.STRING) {
                recordData.setField(ctime, record.op.getTimestamp());
            } else if (recordSchema.getField(ctime).getType() == FieldType.TIMESTAMP) {
                recordData.setField(ctime, convertStrToMicroseconds(record.op.getTimestamp()));
            } else {
                logger.error("BuildRecord failed, cTimeColumn type must be string or timestamp in DataHub, type: {}",
                        recordSchema.getField(ctime).getType().name());
                throw new RuntimeException("cTimeColumn type must be string or timestamp in DataHub");
            }
        }

        String cId = tableMapping.getcIdColumn();
        if (StringUtils.isNotBlank(cId)) {
            recordData.setField(cId, record.recordId);
        }

        Timestamp timestamp = Timestamp.valueOf(record.op.getTimestamp());
        Map<String, String> constMap = tableMapping.getConstColumnMappings();
        if (constMap != null && !constMap.isEmpty()) {
            for (Map.Entry<String, String> entry : constMap.entrySet()) {
                recordData.setField(entry.getKey(), BucketPath.escapeString(entry.getValue(),
                        timestamp.getTime(), tableMapping.getConstColumnMappings()));
            }
        }

        for (int i = 0; i < columns.size(); i++) {

            String columnName = record.op.getTableMeta().getColumnName(i).toLowerCase();
            ColumnMapping columnMapping = tableMapping.getColumnMappings().get(columnName);
            if (columnMapping == null) {
                continue;
            }

            DsColumn dsColumn = columns.get(i);
            String afterValue = dsColumn.hasAfterValue() ? dsColumn.getAfterValue() : null;
            String beforeValue = dsColumn.hasBeforeValue() ? dsColumn.getBeforeValue() : null;

            String dest = columnMapping.getDest();
            String hashVal = null;
            if (StringUtils.isNotBlank(dest)) {
                hashVal = afterValue;
                if (columnMapping.isKeyColumn()) {
                    if (dsColumn.hasAfterValue()) {
                        setTupleData(recordData, recordSchema.getField(dest), afterValue,
                                columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                    } else {
                        setTupleData(recordData, recordSchema.getField(dest), beforeValue,
                                columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                        hashVal = beforeValue;
                    }
                } else {
                    setTupleData(recordData, recordSchema.getField(dest), afterValue,
                            columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
                }
            }

            String destOld = columnMapping.getDestOld();
            if (StringUtils.isNotBlank(destOld)) {
                hashVal = hashVal != null ? hashVal : beforeValue;
                setTupleData(recordData, recordSchema.getField(destOld), beforeValue,
                        columnMapping.isDateFormat(), columnMapping.getSimpleDateFormat());
            }

            if (columnMapping.isShardColumn() && hashVal != null) {
                hashString.append(hashVal);
            }
        }

        if (hashString.length() > 0) {
            recordEntry.setPartitionKey(hashString.toString());
        }
        recordEntry.setRecordData(recordData);
    }

    private void buildBlobRecord(Record record, TableMapping tableMapping, RecordEntry recordEntry) {
        List<DsColumn> columns = record.op.getColumns();
        if (tableMapping.getColumnMappings().size() != 1) {
            logger.error("BuildRecord failed, oracle table must have only one column for blob topic, " +
                            "oracle table: {}, DataHub topic: {}, column num: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), record.op.getNumColumns());
            throw new RuntimeException("oracle table must have only one column for blob topic");
        }

        for (int i = 0; i < columns.size(); i++) {

            String columnName = record.op.getTableMeta().getColumnName(i).toLowerCase();
            ColumnMapping columnMapping = tableMapping.getColumnMappings().get(columnName);
            if (columnMapping == null) {
                continue;
            }

            DsColumn dsColumn = columns.get(i);
            byte[] data;
            if ("D".equalsIgnoreCase(record.opType)) {
                data = dsColumn.getBeforeValue().getBytes(StandardCharsets.UTF_8);
            } else {
                data = dsColumn.getAfterValue().getBytes(StandardCharsets.UTF_8);
            }

            BlobRecordData recordData = new BlobRecordData(data);
            recordEntry.setRecordData(recordData);
            break;
        }

        recordEntry.addAttribute("opType", record.opType);
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
            case INTEGER:
                recordData.setField(field.getName(), Integer.parseInt(val));
                break;
            case FLOAT:
                recordData.setField(field.getName(), Float.parseFloat(val));
                break;
            case TINYINT:
                recordData.setField(field.getName(), (byte) Integer.parseInt(val));
                break;
            case SMALLINT:
                recordData.setField(field.getName(), (short) Integer.parseInt(val));
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

    public static class Record {

        public Record(String opType, String recordId, Op op) {
            this.opType = opType;
            this.recordId = recordId;
            this.op = op;
            Set<String> keys = op.getRecord().getTokenKeys();
            this.rowIdToken = keys.contains(Constant.GG_ROWID_TOKEN)
                    ? op.getRecord().getGGToken(Constant.GG_ROWID_TOKEN)
                    : op.getRecord().getUserToken(Constant.ROWID_TOKEN);
        }

        String opType;
        String recordId;
        Op op;
        DsToken rowIdToken;
    }

    private class BuildTask implements Runnable {
        private final TableMapping tableMapping;

        BuildTask(TableMapping tableMapping) {
            this.tableMapping = tableMapping;
        }

        @Override
        public void run() {
            while (!stop) {
                Record record = null;
                try {
                    record = recordQueue.peek();
                    if (record != null) {
                        RecordEntry recordEntry = buildRecord(record);
                        TopicWriter writer = RecordWriter.instance().getTopicWriter(tableMapping.getOracleFullTableName());
                        writer.writeRecord(recordEntry);
                        recordQueue.poll();
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    logger.error("RecordBuild failed, table: {}", tableMapping.getOracleFullTableName(), e);
                    if (configure.isDirtyDataContinue()) {
                        if (record != null) {
                            BadOperateWriter.write(record.op, tableMapping.getOracleFullTableName(), tableMapping.getTopicName(),
                                    configure.getDirtyDataFile(), configure.getDirtyDataFileMaxSize(), e.getMessage());
                        }
                    } else {
                        logger.error("RecordBuild failed, will stop...");
                        stop = true;
                    }
                }
            }
        }
    }
}
