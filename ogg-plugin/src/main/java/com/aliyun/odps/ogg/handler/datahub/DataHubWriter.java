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

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DataHubWriter {
    private static final Logger logger = LoggerFactory.getLogger(DataHubWriter.class);

    private Map<String, List<RecordEntry>> recordCache = Maps.newHashMap();
    private Configure configure;

    private static DataHubWriter dataHubWriter;
    private DatahubClient client;

    public static DataHubWriter instance() {
        return dataHubWriter;
    }

    public static void init(Configure configure) {
        if (dataHubWriter == null) {
            dataHubWriter = new DataHubWriter(configure);
        }
    }

    private DataHubWriter(Configure configure) {
        this.configure = configure;
        initDataHub();

        checkTableMapping();
    }

    private void initDataHub() {
        HttpConfig httpConfig = new HttpConfig();
        if (StringUtils.isNotBlank(configure.getCompressType())) {
            boolean flag = false;
            for (HttpConfig.CompressType type : HttpConfig.CompressType.values()) {
                if (type.name().equals(configure.getCompressType())) {
                    httpConfig.setCompressType(HttpConfig.CompressType.valueOf(configure.getCompressType()));
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                logger.warn("DataHubWriter, invalid DataHub compressType, deploy no compress, compressType: {}",
                        configure.getCompressType());
            }
        }

        client = DatahubClientBuilder.newBuilder()
                .setUserAgent(Constant.PLUGIN_VERSION)
                .setDatahubConfig(
                        new DatahubConfig(configure.getDatahubEndpoint(),
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(configure.getDatahubAccessId(), configure.getDatahubAccessKey()),
                                configure.isEnablePb()))
                .setHttpConfig(httpConfig)
                .build();

        // init RecordSchema and shardList
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableMapping tableMapping = entry.getValue();
            GetTopicResult topic = client.getTopic(tableMapping.getProjectName(), tableMapping.getTopicName());
            if (topic.getRecordType() == RecordType.TUPLE) {
                tableMapping.setRecordSchema(topic.getRecordSchema());
            }
            freshShardList(tableMapping);
            recordCache.put(entry.getKey(), new ArrayList<RecordEntry>());
        }
    }

    private void checkTableMapping() {
        for (TableMapping tableMapping : configure.getTableMappings().values()) {
            checkColumnMapping(tableMapping);
        }
    }

    private void checkColumnMapping(TableMapping tableMapping) {
        if (tableMapping.getRecordSchema() == null) {
            return;
        }


        checkColumn(tableMapping.getRowIdColumn(), Arrays.asList(FieldType.STRING),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcTypeColumn(), Arrays.asList(FieldType.STRING),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcTimeColumn(), Arrays.asList(FieldType.STRING,
                FieldType.TIMESTAMP), tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcIdColumn(), Arrays.asList(FieldType.STRING),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        for (Map.Entry<String, String> entry : tableMapping.getConstColumnMappings().entrySet()) {
            checkColumn(entry.getKey(), Arrays.asList(FieldType.STRING),
                    tableMapping.getRecordSchema(), tableMapping.getTopicName());
        }

        for (Map.Entry<String, ColumnMapping> entry : tableMapping.getColumnMappings().entrySet()) {
            ColumnMapping columnMapping = entry.getValue();

            checkColumn(columnMapping.getDest(), null, tableMapping.getRecordSchema(), tableMapping.getTopicName());
            checkColumn(columnMapping.getDestOld(), null, tableMapping.getRecordSchema(), tableMapping.getTopicName());
        }
    }

    private void checkColumn(String columnName, List<FieldType> types, RecordSchema recordSchema, String topicName) {
        if (StringUtils.isBlank(columnName)) {
            return;
        }

        Field field = recordSchema.getField(columnName);
        if (field == null) {
            logger.error("CheckSchema failed, the field is not exist in DataHub, topic: {}, fieldName: {}", topicName, columnName);
            throw new IllegalArgumentException("the field is not exist in DataHub");
        }

        if (types != null) {
            boolean flag = false;
            for (FieldType type : types) {
                if (type == field.getType()) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                logger.error("CheckSchema failed, the oracle column corresponding field type is invalid in DataHub," +
                        " column: {}, filedType: {}", columnName, field.getType().name());
                throw new IllegalArgumentException("the oracle column corresponding field type is invalid in DataHub");
            }
        }
    }

    private void freshShardList(TableMapping tableMapping) {
        if (tableMapping.isSetShardId()) {
            return;
        }

        ListShardResult result = client.listShard(tableMapping.getProjectName(), tableMapping.getTopicName());
        List<String> shardIds = new ArrayList<String>();

        for (ShardEntry entry : result.getShards()) {
            if (entry.getState() == ShardState.ACTIVE) {
                shardIds.add(entry.getShardId());
            }
        }
        tableMapping.setShardIds(shardIds);
    }

    public void addRecord(String oracleFullTableName, RecordEntry recordEntry) {
        List<RecordEntry> recordEntries = this.recordCache.get(oracleFullTableName);

        recordEntries.add(recordEntry);

        if (recordEntries.size() >= configure.getBatchSize()) {
            this.writeToHub(oracleFullTableName);
        }
    }

    private void initRecordShardIds(String oracleFullTableName, List<RecordEntry> recordEntries) {

        TableMapping tableMapping = this.configure.getTableMapping(oracleFullTableName);

        // if not set shardId, and set hash
        if (!tableMapping.isSetShardId() && tableMapping.isShardHash()) {
            return;
        }

        String shardId = tableMapping.getShardId();
        for (RecordEntry recordEntry : recordEntries) {
            recordEntry.setShardId(shardId);
        }
    }

    //TODO 可以多线程优化写性能
    public void flushAll() {
        for (String oracleFullTableName : recordCache.keySet()) {
            this.writeToHub(oracleFullTableName);
        }
    }

    private void writeToHub(String oracleFullTableName) {
        List<RecordEntry> recordEntries = this.recordCache.get(oracleFullTableName);
        if (recordEntries == null || recordEntries.size() == 0) {
            return;
        }

        initRecordShardIds(oracleFullTableName, recordEntries);
        TableMapping tableMapping = this.configure.getTableMapping(oracleFullTableName);

        int retryCount = 0;
        while (true) {
            String errorMessage = "";
            PutRecordsResult putRecordsResult = null;
            try {
                putRecordsResult = client.putRecords(tableMapping.getProjectName(), tableMapping.getTopicName(), recordEntries);
            } catch (DatahubClientException e) {
                logger.error("DataHubWriter failed, put records to DataHub failed. table: {}, topic: {}, recordNum: {}",
                        oracleFullTableName, tableMapping.getTopicName(), recordEntries.size(), e);
                errorMessage = e.getErrorMessage();
            }

            if (putRecordsResult != null && putRecordsResult.getFailedRecordCount() == 0) {
                logger.info("DataHubWriter Success, put records to DataHub Success, table: {}, topic: {}, recordNum: {}",
                        oracleFullTableName, tableMapping.getTopicName(), recordEntries.size());

                // save checkpoints
                HandlerInfoManager.instance().saveHandlerInfos();
                PluginStatictics.addSendTimesInTx();
                this.recordCache.get(oracleFullTableName).clear();
                break;
            }

            if (configure.getRetryTimes() < 0 || retryCount < configure.getRetryTimes()) {
                if (putRecordsResult != null && putRecordsResult.getFailedRecordCount() > 0) {
                    logger.error("DataHubWriter failed, put records to DataHub failed," +
                                    " table: {}, topic: {}, failedNum: {}, ErrorCode: {}, Message : {}",
                            oracleFullTableName, tableMapping.getTopicName(), putRecordsResult.getFailedRecordCount(),
                            putRecordsResult.getPutErrorEntries().get(0).getErrorcode(),
                            putRecordsResult.getPutErrorEntries().get(0).getMessage());
                    recordEntries.clear();
                    List<RecordEntry> failedRecords = putRecordsResult.getFailedRecords();

                    boolean needFresh = true;
                    String shardId = "";
                    for (int i = 0; i < failedRecords.size(); ++i) {
                        RecordEntry entry = failedRecords.get(i);
                        PutErrorEntry errorEntry = putRecordsResult.getPutErrorEntries().get(i);

                        if ("MalformedRecord".equals(errorEntry.getErrorcode())) {
                            if (configure.isDirtyDataContinue()) {
                                BadOperateWriter.write(entry, oracleFullTableName, tableMapping.getTopicName(),
                                        configure.getDirtyDataFile(),
                                        configure.getDirtyDataFileMaxSize(), errorEntry.getMessage());
                            } else {
                                logger.error("DataHubWriter failed, put records to DataHub failed, found dirty data, " +
                                                "table: {}, topic: {}, record: {}",
                                        oracleFullTableName, tableMapping.getTopicName(), JsonHelper.beanToJson(entry));
                                throw new RuntimeException("put records to DataHub failed, found dirty data");
                            }
                        }

                        // reset shardId if need
                        if ("InvalidShardOperation".equals(errorEntry.getErrorcode())) {
                            if (needFresh) {
                                freshShardList(tableMapping);
                                shardId = tableMapping.getShardId();
                                needFresh = false;
                            }
                            entry.setShardId(shardId);
                        }
                        recordEntries.add(entry);
                    }
                }
                logger.warn("DataHubWriter, put records to DataHub failed, will retry after {} ms, table: {}, topic: {}",
                        configure.getRetryInterval(), oracleFullTableName, tableMapping.getTopicName());
                try {
                    Thread.sleep(configure.getRetryInterval());
                } catch (InterruptedException e1) {
                    // Do nothing
                }
            } else { // no retry or retry count exceed.

                logger.error("DataHubWriter failed, put records to DataHub failed, table: {}, topic: {}",
                        oracleFullTableName, tableMapping.getTopicName());

                if (configure.isDirtyDataContinue()) {
                    // put record failed for putRecord throw exception
                    if (putRecordsResult == null) {
                        for (RecordEntry entry : recordEntries) {
                            BadOperateWriter.write(entry, oracleFullTableName,
                                    tableMapping.getTopicName(), configure.getDirtyDataFile(),
                                    configure.getDirtyDataFileMaxSize(), errorMessage);
                        }
                    } else {
                        for (int i = 0; i < putRecordsResult.getFailedRecordCount(); ++i) {
                            BadOperateWriter.write(putRecordsResult.getFailedRecords().get(i), oracleFullTableName,
                                    tableMapping.getTopicName(), configure.getDirtyDataFile(),
                                    configure.getDirtyDataFileMaxSize(),
                                    putRecordsResult.getPutErrorEntries().get(i).getMessage());
                        }
                    }
                } else {
                    if (putRecordsResult != null) {
                        errorMessage = putRecordsResult.getPutErrorEntries().get(0).getMessage();
                    }

                    throw new RuntimeException("put records to DataHub failed, " + errorMessage);
                }
                this.recordCache.get(oracleFullTableName).clear();
                break;
            }
            retryCount++;
        }
    }


    /**
     * only used in unit test
     */
    static void reInit(Configure configure) {
        dataHubWriter = new DataHubWriter(configure);
    }

    public DatahubClient getDataHubClient() {
        return client;
    }
}
