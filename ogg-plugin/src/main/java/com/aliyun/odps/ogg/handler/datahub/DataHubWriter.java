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

import com.aliyun.datahub.model.*;
import com.aliyun.datahub.wrapper.Topic;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataHubWriter {
    private static final Logger logger = LoggerFactory.getLogger(DataHubWriter.class);

    private Map<String, List<RecordEntry>> recordCache = Maps.newHashMap();
    private Map<String, Integer> shardIndexs = Maps.newHashMap();
    private Map<String, List<String>> shardIdLists = Maps.newHashMap();
    private Configure configure;

    private static DataHubWriter dataHubWriter;

    public static DataHubWriter instance() {
        return dataHubWriter;
    }

    public static void init(Configure configure) {
        if (dataHubWriter == null) {
            dataHubWriter = new DataHubWriter(configure);
        }
    }

    // only used in unit test
    static void reInit(Configure configure) {
        dataHubWriter = new DataHubWriter(configure);
    }

    private DataHubWriter(Configure configure) {
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableMapping tableMapping = entry.getValue();
            Topic topic = entry.getValue().getTopic();
            List<String> shardIds = Lists.newArrayList();
            List<ShardEntry> shardEntries = topic.listShard();

            for (ShardEntry shardEntry : shardEntries) {
                if (tableMapping.getShardId() != null) {
                    if (tableMapping.getShardId().equals(shardEntry.getShardId())) {
                        if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                            shardIds.add(shardEntry.getShardId());
                        }
                        break;
                    }
                } else {
                    if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                        shardIds.add(shardEntry.getShardId());
                    }
                }

            }

            if (shardIds.size() == 0) {
                throw new RuntimeException(
                    "Topic[" + topic.getTopicName() + "] has not active shard");
            }

            shardIdLists.put(entry.getKey(), shardIds);
            recordCache.put(entry.getKey(), new ArrayList<RecordEntry>());
            shardIndexs.put(entry.getKey(), 0);
        }

        this.configure = configure;

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
        List<String> shardIds = this.shardIdLists.get(oracleFullTableName);
        Integer index = this.shardIndexs.get(oracleFullTableName) % shardIds.size();

        for (RecordEntry recordEntry : recordEntries) {
            if (tableMapping.getShardId() != null) {
                recordEntry.setShardId(tableMapping.getShardId());
            } else if (tableMapping.isShardHash()) {
                int hashCode = Integer.valueOf(recordEntry.getAttributes().get(Constant.HASH));
                recordEntry.setShardId(shardIds.get(hashCode % shardIds.size()));
            } else {
                index = index % shardIds.size();
                recordEntry.setShardId(shardIds.get(index));
                index++;
            }
        }

        this.shardIndexs.put(oracleFullTableName, index);

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

        TableMapping tableMapping = this.configure.getTableMapping(oracleFullTableName);
        Topic topic = tableMapping.getTopic();
        int retryCount = 0;
        this.initRecordShardIds(oracleFullTableName, recordEntries);
        List<ErrorEntry> errorEntries = Lists.newArrayList();
        while (true) {
            String errorMessage = "";
            try {
                PutRecordsResult putRecordsResult = topic.putRecords(recordEntries);
                //write had fail records
                if (putRecordsResult.getFailedRecordCount() > 0) {
                    recordEntries = putRecordsResult.getFailedRecords();
                    errorEntries = putRecordsResult.getFailedRecordError();
                    //需要重试
                    if (configure.getRetryTimes() < 0 || (configure.getRetryTimes() >= 0
                        && retryCount < configure.getRetryTimes())) {
                        //轮训写shard, 需要重新判断shard的状态
                        if (tableMapping.getShardId() == null || tableMapping.isShardHash()) {

                            for (ErrorEntry errorEntry : errorEntries) {
                                //有无效的shard id
                                if ("InvalidShardOperation"
                                    .equalsIgnoreCase(errorEntry.getErrorcode())) {
                                    List<ShardEntry> shardEntries = topic.listShard();
                                    List<String> shardIds = Lists.newArrayList();

                                    for (ShardEntry shardEntry : shardEntries) {
                                        if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                                            shardIds.add(shardEntry.getShardId());
                                        }
                                    }
                                    //没有可用的shard 使用以前的shard接着写
                                    if (shardIds.size() > 0) {
                                        this.shardIdLists.put(oracleFullTableName, shardIds);

                                        this.initRecordShardIds(oracleFullTableName, recordEntries);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Write " + String.valueOf(recordEntries.size()) + " record to topic["
                                + topic.getTopicName() + "] success");
                    }

                    // save checkpoints
                    HandlerInfoManager.instance().saveHandlerInfos();
                    PluginStatictics.addSendTimesInTx();
                    this.recordCache.get(oracleFullTableName).clear();
                    break;
                }
            } catch (Exception e) {
                logger.warn("Write record to topic[" + topic.getTopicName() + "] failed", e);
                errorMessage = e.getMessage();
            }

            if (configure.getRetryTimes() >= 0 && retryCount >= configure.getRetryTimes()) {
                if (configure.isDirtyDataContinue()) {
                    for (int i = 0; i < recordEntries.size(); i++) {
                        if (errorEntries.size() > 0) {
                            errorMessage = errorEntries.get(i).getMessage();
                        }

                        BadOperateWriter
                            .write(recordEntries.get(i), oracleFullTableName, topic.getTopicName(),
                                configure.getDirtyDataFile(),
                                configure.getDirtyDataFileMaxSize() * 1000000, errorMessage);
                    }

                    this.recordCache.get(oracleFullTableName).clear();

                    break;
                } else {
                    throw new RuntimeException(
                        "Write record to topic[" + topic.getTopicName() + "] failed");
                }

            }

            logger.warn(
                "Write record to topic[" + topic.getTopicName() + "] failed, will retry after "
                    + configure.getRetryInterval() + "ms");
            try {
                Thread.sleep(configure.getRetryInterval());
            } catch (InterruptedException e1) {
                // Do nothing
            }
            if (errorEntries != null) {
                errorEntries.clear();
            }
            retryCount++;
        }
    }
}
