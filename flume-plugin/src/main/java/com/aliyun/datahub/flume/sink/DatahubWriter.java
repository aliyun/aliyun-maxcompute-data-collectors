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

package com.aliyun.datahub.flume.sink;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DatahubWriter {
    private static final Logger logger = LoggerFactory.getLogger(DatahubWriter.class);
    private DatahubClient datahubClient;
    private GetTopicResult topic;
    private Configure configure;
    private FileWriter writer;
    private List<String> activeShardIds;
    private long threadId;


    public DatahubWriter(Configure configure) throws IOException {
        this.configure = configure;
        init();
    }

    public void close() throws IOException {
        if (configure.isDirtyDataContinue()) {
            writer.close();
        }
    }

    private void init() throws IOException {
        HttpConfig config = new HttpConfig();
        if (configure.getCompressType() != null) {
            config.setCompressType(HttpConfig.CompressType.valueOf(configure.getCompressType()));
        }
        datahubClient = DatahubClientBuilder.newBuilder()
                .setHttpConfig(config)
                .setUserAgent("datahub-flume-plugin-2.0.0")
                .setDatahubConfig(
                        new DatahubConfig(configure.getEndPoint(),
                                new com.aliyun.datahub.client.auth.AliyunAccount(
                                        configure.getAccessId(),
                                        configure.getAccessKey()),
                                configure.isEnablePb()))
                .build();

        topic = datahubClient.getTopic(configure.getProject(), configure.getTopic());
        checkSchema();

        freshActiveShardList();

        if (configure.isDirtyDataContinue()) {
            writer = new FileWriter(configure.getDirtyDataFile(), true);
        }
    }

    public void checkSchema() {
        RecordSchema schema = topic.getRecordSchema();
        String[] columns = configure.getInputColumnNames();
        for (String columnName : columns) {
            if (!StringUtils.isEmpty(columnName) && !schema.containsField(columnName)) {
                throw new IllegalArgumentException("The field " + columnName + " is not exist in datahub schema.");
            }
        }
    }

    public int writeRecords(List<RecordEntry> recordEntries) throws IOException {
        threadId = Thread.currentThread().getId();
        return putRecordWithRetry(recordEntries);
    }

    public TupleRecordData buildRecord(Map<String, String> rowMap) throws IOException {
        threadId = Thread.currentThread().getId();
        TupleRecordData data = new TupleRecordData(topic.getRecordSchema());
        try {
            for (Map.Entry<String, String> mapEntry : rowMap.entrySet()) {
                String fieldName = mapEntry.getKey();

                Field field = topic.getRecordSchema().getField(fieldName);
                String val = mapEntry.getValue().trim();
                if (!"null".equalsIgnoreCase(val)) {
                    switch (field.getType()) {
                        case STRING:
                            data.setField(fieldName, val);
                            break;
                        case BIGINT:
                        case TIMESTAMP:
                            data.setField(fieldName, Long.parseLong(val));
                            break;
                        case DOUBLE:
                            data.setField(fieldName, Double.parseDouble(val));
                            break;
                        case BOOLEAN:
                            data.setField(fieldName, Boolean.parseBoolean(val));
                            break;
                        case DECIMAL:
                            data.setField(fieldName, new BigDecimal(val));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported RecordType " + field.getType().name());
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("[Thread {}] Build record failed. ", threadId, e);
            handleDirtyData(rowMap);
            return null;
        }
        return data;
    }

    private void freshActiveShardList() {
        List<String> shardIds = configure.getShardIds();
        if (shardIds != null && !shardIds.isEmpty()) {
            activeShardIds = shardIds;
            return;
        }

        if (activeShardIds == null) {
            activeShardIds = new ArrayList<String>();
        }

        activeShardIds.clear();
        ListShardResult result = datahubClient
                .listShard(configure.getProject(), configure.getTopic());
        for (ShardEntry shardEntry : result.getShards()) {
            if (shardEntry.getState() == ShardState.ACTIVE) {
                activeShardIds.add(shardEntry.getShardId());
            }
        }

        if (activeShardIds.isEmpty()) {
            throw new IllegalArgumentException("The specific topic " + configure.getTopic() + " has no active shard.");
        }
    }

    public String getActiveShardId() {
        int index = new Random().nextInt(activeShardIds.size());
        return activeShardIds.get(index);
    }

    private int putRecordWithRetry(List<RecordEntry> recordEntries) throws IOException {
        int putSucNum = 0;
        int retryNum = configure.getRetryTimes();
        int interval = configure.getRetryInterval();
        while (true) {
            PutRecordsResult result = null;
            try {
                result = datahubClient.putRecords(configure.getProject(),
                        configure.getTopic(), recordEntries);
            } catch (DatahubClientException e) {
                logger.error("[Thread {}] Put {} records to DataHub failed. {}",
                        threadId, recordEntries.size(), e.getErrorMessage());
                //throw e;
            }

            if (result != null) {
                putSucNum += recordEntries.size() - result.getFailedRecordCount();
                if (result.getFailedRecordCount() == 0) {
                    logger.info("[Thread {}] Put {} records to DataHub successful.",
                            threadId, recordEntries.size());
                    break;
                }
            }

            if (retryNum > 0) {
                try {
                    Thread.sleep(interval * 1000);
                } catch (InterruptedException e) {
                }
                logger.warn("[Thread {} ] Now retry ({})...", threadId, retryNum);

                if (result != null && result.getFailedRecordCount() > 0) {
                    logger.warn("[Thread {} ] Put {} records to DataHub. {} records is failed. {}",
                            threadId, recordEntries.size(), result.getFailedRecordCount(),
                            result.getPutErrorEntries().get(0).getMessage());

                    recordEntries.clear();
                    List<RecordEntry> failedRecords = result.getFailedRecords();

                    boolean needFresh = true;
                    String shardId = "";
                    for (int i = 0; i < failedRecords.size(); ++i) {
                        RecordEntry entry = failedRecords.get(i);
                        PutErrorEntry errorEntry = result.getPutErrorEntries().get(i);

                        if (errorEntry.getErrorcode().equals("MalformedRecord")) {
                            handleDirtyData(entry);
                            continue;
                        }

                        // reset shardId if need
                        if (errorEntry.getErrorcode().equals("InvalidShardOperation")) {
                            if (needFresh) {
                                freshActiveShardList();
                                shardId = getActiveShardId();
                                needFresh = false;
                            }
                            entry.setShardId(shardId);
                        }
                        recordEntries.add(entry);
                    }
                }
                retryNum--;
            } else {
                if (result != null && result.getFailedRecordCount() > 0) {
                    recordEntries = result.getFailedRecords();
                    logger.error("[Thread {} ] {} records put failed. {}", threadId,
                            recordEntries.size(), result.getPutErrorEntries().get(0).getMessage());
                    throw new RuntimeException("put record failed. "
                            + result.getPutErrorEntries().get(0).getMessage());
                }
                break;
            }
        }
        return putSucNum;
    }

    public void handleDirtyData(String rawBody) throws IOException {
        threadId = Thread.currentThread().getId();
        if (!configure.isDirtyDataContinue()) {
            logger.error("[Thread {}] Dirty data found, exit process now.", threadId);
            throw new RuntimeException("Dirty data found, exit process now.");
        }

        logger.warn("[Thread {} ] Dirty data found, will write to dirtyDataFile {}",
                threadId, configure.getDirtyDataFile());

        writer.write(rawBody + "\n");
        writer.flush();
    }

    private void handleDirtyData(Map<String, String> rowMap) throws IOException {
        if (!configure.isDirtyDataContinue()) {
            logger.error("[Thread {}] Dirty data found, exit process now.", threadId);
            throw new RuntimeException("Dirty data found, exit process now.");
        }

        logger.warn("[Thread {} ] Dirty data found, will write to dirtyDataFile {}",
                threadId, configure.getDirtyDataFile());
        StringBuilder builder = new StringBuilder();
        String[] columnNames = configure.getInputColumnNames();

        for (int i = 0; i < columnNames.length; ++i) {
            builder.append(rowMap.get(columnNames[i]));
            if (i != columnNames.length - 1) {
                builder.append(",");
            }
        }
        builder.append("\n");
        writer.write(builder.toString());
        writer.flush();
    }

    private void handleDirtyData(RecordEntry entry) throws IOException {
        if (!configure.isDirtyDataContinue()) {
            logger.error("[Thread {}] Dirty data found, exit process now.", threadId);
            throw new RuntimeException("Dirty data found, exit process now.");
        }

        logger.warn("[Thread {} ] Dirty data found, will write to dirtyDataFile {}",
                threadId, configure.getDirtyDataFile());

        TupleRecordData data = (TupleRecordData) entry.getRecordData();
        StringBuilder builder = new StringBuilder();
        String[] columnNames = configure.getInputColumnNames();

        for (int i = 0; i < columnNames.length; ++i) {
            builder.append(data.getField(columnNames[i]));
            if (i != columnNames.length - 1) {
                builder.append(",");
            }
        }
        builder.append("\n");
        writer.write(builder.toString());
        writer.flush();
    }
}
