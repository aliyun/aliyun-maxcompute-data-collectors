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

import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class TopicWriter {
    private static final Logger logger = LoggerFactory.getLogger(TopicWriter.class);
    private final Object writerLock = new Object();

    private Configure configure;
    private TableMapping tableMapping;
    private ExecutorService executor;

    private List<ShardWriter> shardWriters;

    private long sequence = 0;
    private int index = 0;
    private volatile boolean updateShards = false;

    public TopicWriter(Configure configure, TableMapping tableMapping, ExecutorService executor) {
        this.configure = configure;
        this.tableMapping = tableMapping;
        this.executor = executor;
    }

    public void start() {
        updateShardWriters();
    }

    public void stop() {
        for (ShardWriter shardWriter : shardWriters) {
            shardWriter.close();
        }
    }

    public void syncExec() {
        try {
            doSyncExec();
        } catch (ShardSealedException e) {
            logger.warn("Shard changed, restart shard writer, table: {}, topic: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
            updateShardWriters();
            doSyncExec();
        }
    }

    private void doSyncExec() {
        for (ShardWriter shardWriter : shardWriters) {
            shardWriter.syncExec();
        }
    }

    public void writeRecord(RecordEntry recordEntry) {
        try {
            if (!updateShards) {
                doWriteRecord(recordEntry);
            } else {
                synchronized (writerLock) {
                    doWriteRecord(recordEntry);
                }
            }
        } catch (ShardSealedException e) {
            logger.warn("Shard changed, restart shard writer, table: {}, topic: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
            updateShardWriters();
            writeRecord(recordEntry);
        }
    }

    private void doWriteRecord(RecordEntry recordEntry) {
        recordEntry.setSequence(sequence);
        int idx = getWriteIndex(recordEntry);
        shardWriters.get(idx).writeRecord(recordEntry);
        if (++sequence % configure.getBatchSize() == 0) {
            ++index;
        }
    }

    private int getWriteIndex(RecordEntry recordEntry) {
        String partitionKey = recordEntry.getPartitionKey();
        if (partitionKey == null) {
            return ((index % shardWriters.size()) + shardWriters.size()) % shardWriters.size();
        } else {
            return (partitionKey.hashCode() % shardWriters.size() + shardWriters.size()) % shardWriters.size();
        }
    }

    private void updateShardWriters() {
        synchronized (writerLock) {
            updateShards = true;
            logger.warn("Start update shardWriters..., table: {}, topic: {}", tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
            List<RecordEntry> bufferRecords = new ArrayList<>();
            if (shardWriters != null && !shardWriters.isEmpty()) {
                for (ShardWriter shardWriter : shardWriters) {
                    RecordBatchQueue queue = shardWriter.getBatchQueue();
                    while (!queue.isEmpty()) {
                        RecordBatch batch = queue.peek();
                        bufferRecords.addAll(batch.getRecords());
                        queue.pop();
                    }
                    bufferRecords.addAll(shardWriter.getRecordBatch().getRecords());
                }
            }

            List<String> shardIds = ClientHelper.instance().getShardList(tableMapping.getProjectName(), tableMapping.getTopicName());
            List<ShardWriter> newShardWriters = new ArrayList<>();
            for (String shardId : shardIds) {
                ShardWriter shardWriter = new ShardWriter(configure, executor, tableMapping.getOracleTableName(),
                        tableMapping.getProjectName(), tableMapping.getTopicName(), shardId);
                shardWriter.start();
                newShardWriters.add(shardWriter);
            }
            this.shardWriters = newShardWriters;

            if (!bufferRecords.isEmpty()) {
                bufferRecords.sort(Comparator.comparingLong(RecordEntry::getSequence));
                for (RecordEntry bufferRecord : bufferRecords) {
                    writeRecord(bufferRecord);
                }
            }
            updateShards = false;
            logger.warn("Update shardWriters success, table: {}, topic: {}", tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
        }
    }
}
