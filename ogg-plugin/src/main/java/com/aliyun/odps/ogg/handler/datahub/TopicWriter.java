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
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.PutErrorEntry;
import com.aliyun.datahub.client.model.PutRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.MetricHelper;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TopicWriter {
    private static final Logger logger = LoggerFactory.getLogger(TopicWriter.class);

    private static final int LIST_SHARD_RETRY_TIMES = 30;
    private static final int LIST_SHARD_RETRY_INTERVAL_MS = 1000;
    private static final int SCHEDULE_FLUSHER_INTERVAL_MS = 1000;
    private static final int SCHEDULED_MONITOR_INTERVAL_MS = 60 * 1000;
    private static final int SYNC_LOCK_WAIT_TIMEOUT_MS = 1000;
    private final Object writerLock = new Object();

    private Configure configure;
    private TableMapping tableMapping;
    private DatahubClient client;
    private long sequence = 0;
    private int index = 0;
    private ScheduledExecutorService scheduledExecutorService;
    private List<ShardWriter> shardWriters;

    public TopicWriter(Configure configure, TableMapping tableMapping, DatahubClient client) {
        this.configure = configure;
        this.tableMapping = tableMapping;
        this.client = client;

        scheduledExecutorService = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern(tableMapping.getTopicName() + ".Writer.Schedule-%d").daemon(true).build());
    }

    public void start() {
        updateShardWriters();

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            flush();
        }, SCHEDULE_FLUSHER_INTERVAL_MS, SCHEDULE_FLUSHER_INTERVAL_MS, TimeUnit.MILLISECONDS);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            if (isShardChanged()) {
                updateShardWriters();
            }
        }, SCHEDULED_MONITOR_INTERVAL_MS, SCHEDULED_MONITOR_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }


    public void stop() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }

        sync();
        for (ShardWriter shardWriter : shardWriters) {
            shardWriter.stop();
        }
    }

    public synchronized void writeRecord(RecordEntry recordEntry) {
        if (isShardChanged()) {
            logger.warn("Shard changed, restart shard writer, table: {}, topicName: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
            updateShardWriters();
        }

        recordEntry.setSequence(sequence);
        int idx = getWriteIndex(recordEntry);
        shardWriters.get(idx).write(recordEntry);
        if (++sequence % configure.getBatchSize() == 0) {
            ++index;
        }
    }

    public void sync() {
        try {
            for (ShardWriter writer : shardWriters) {
                writer.sync();
            }
        } catch (ShardSealedException e) {
            logger.warn("Sync write DataHub failed, shard is closed, table: {}, topic: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), e);
            updateShardWriters();

        }
    }

    private void flush() {
        synchronized (writerLock) {
            for (ShardWriter shardWriter : shardWriters) {
                shardWriter.flush();
            }
            ++index;
        }
    }

    private boolean isShardChanged() {
        for (ShardWriter shardWriter : shardWriters) {
            if (shardWriter.isShardClosed) {
                return true;
            }
        }
        return false;
    }

    private int getWriteIndex(RecordEntry recordEntry) {
        String partitionKey = recordEntry.getPartitionKey();
        if (partitionKey == null) {
            return ((index % shardWriters.size()) + shardWriters.size()) % shardWriters.size();
        } else {
            return (partitionKey.hashCode() % shardWriters.size() + shardWriters.size()) % shardWriters.size();
        }
    }

    private List<String> getShardList() {
        int retryCount = 0;
        while (true) {
            try {
                List<ShardEntry> shardEntries = client.listShard(tableMapping.getProjectName(), tableMapping.getTopicName()).getShards();
                for (int i = 0; i < LIST_SHARD_RETRY_TIMES; ++i) {
                    if (isShardReady(shardEntries)) {
                        break;
                    }
                    if (i >= LIST_SHARD_RETRY_TIMES - 1) {
                        throw new DatahubClientException("Shard state not ready timeout");
                    }
                    logger.warn("Shard state not ready, will retry [{}], table: {}, topicName: {}",
                            retryCount, tableMapping.getOracleFullTableName(), tableMapping.getTopicName());
                    try {
                        Thread.sleep(LIST_SHARD_RETRY_INTERVAL_MS);
                    } catch (InterruptedException ignored) {
                    }

                }
                shardEntries.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getShardId())));
                List<String> shardIds = new ArrayList<>();

                for (ShardEntry shardEntry : shardEntries) {
                    if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                        shardIds.add(shardEntry.getShardId());
                    }
                }

                return shardIds;
            } catch (DatahubClientException e) {
                if (configure.getRetryTimes() > -1 && retryCount >= configure.getRetryTimes()) {
                    logger.error("List shard failed, table: {}, topic: {}", tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), e);
                    throw e;
                } else {
                    logger.warn("List shard failed, will retry [{}], table: {}, topicName: {}",
                            retryCount, tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), e);
                    ++retryCount;
                }
                try {
                    Thread.sleep(configure.getRetryIntervalMs());
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private boolean isShardReady(List<ShardEntry> shardEntries) {
        for (ShardEntry shardEntry : shardEntries) {
            if (ShardState.CLOSING.equals(shardEntry.getState()) ||
                    ShardState.OPENING.equals(shardEntry.getState())) {
                return false;
            }
        }
        return true;
    }

    private void updateShardWriters() {
        synchronized (writerLock) {
            logger.warn("Update shardWriters...");
            List<RecordEntry> buffRecords = new ArrayList<>();
            if (shardWriters != null && !shardWriters.isEmpty()) {
                for (ShardWriter shardWriter : shardWriters) {
                    while (!shardWriter.batchQueue.isEmpty()) {
                        RecordBatch batch = shardWriter.batchQueue.peek();
                        buffRecords.addAll(batch.getRecords());
                        shardWriter.batchQueue.pop();
                    }
                    buffRecords.addAll(shardWriter.recordBatch.getRecords());
                }
            }

            List<String> shardIds = getShardList();
            List<ShardWriter> newShardWriters = new ArrayList<>();
            for (String shardId : shardIds) {
                ShardWriter shardWriter = new ShardWriter(shardId);
                shardWriter.start();
                newShardWriters.add(shardWriter);
            }
            this.shardWriters = newShardWriters;

            if (!buffRecords.isEmpty()) {
                buffRecords.sort(Comparator.comparingLong(RecordEntry::getSequence));
                for (RecordEntry buffRecord : buffRecords) {
                    writeRecord(buffRecord);
                }
            }
            logger.warn("Update shardWriters success");
        }
    }

    private class ShardWriter {
        private final Logger logger = LoggerFactory.getLogger(ShardWriter.class);

        private String shardId;
        private RecordBatch recordBatch = new RecordBatch();
        private RecordBatchQueue batchQueue = new RecordBatchQueue(configure.getPutRecordQueueSize());
        private Exception exception;
        private final Object batchLock = new Object();
        private final Object syncLock = new Object();
        private boolean isShardClosed = false;
        private volatile boolean stop = false;
        private volatile boolean sync = false;
        private final Thread writeThread;

        private ShardWriter(String shardId) {
            this.shardId = shardId;
            writeThread = new Thread(this::run, tableMapping.getTopicName() + "-ShardWriter-" + shardId);
        }

        private void start() {
            writeThread.start();
        }

        private void stop() {
            if (!stop) {
                stop = true;
                writeThread.interrupt();
            }
        }

        private void write(RecordEntry recordEntry) {
            synchronized (batchLock) {
                recordBatch.add(recordEntry);
                if (isReady(recordBatch)) {
                    submit();
                }
            }
        }

        private void flush() {
            synchronized (batchLock) {
                if (isReady(recordBatch)) {
                    submit();
                }
            }
        }

        private void sync() {
            synchronized (batchLock) {
                submit();
            }

            try {
                if (!batchQueue.isEmpty()) {
                    synchronized (syncLock) {
                        sync = true;
                        syncLock.wait();
                        while (!batchQueue.isEmpty()) {
                            RecordBatch recordBatch = batchQueue.peek();
                            doWrite(recordBatch);
                            batchQueue.pop();
                        }
                        sync = false;
                        syncLock.notify();
                    }
                }
            } catch (InterruptedException e) {
                logger.error("sync put all record failed, table: {}, topic: {}",
                        tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), e);
                throw new RuntimeException(e.getMessage());
            }
        }

        private void submit() {
            boolean success = false;
            try {
                while (!success && exception == null) {
                    success = batchQueue.offer(recordBatch, configure.getRetryIntervalMs(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ignored) {
            }

            if (exception != null) {
                throw new RuntimeException(exception.getMessage());
            }
            recordBatch = new RecordBatch();
        }

        private boolean isReady(RecordBatch batch) {
            return batch.getRecords().size() >= configure.getBatchSize()
                    || System.currentTimeMillis() - batch.getCreateTimestamp() >= configure.getBatchTimeoutMs();
        }

        private void run() {
            logger.info("ShardWriter started, table: {}, topicName: {}, shardId: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId);

            try {
                RecordBatch recordBatch = null;
                while (!stop) {
                    try {
                        if (sync) {
                            synchronized (syncLock) {
                                syncLock.notify();
                                // wait timeout for prevent deadlock
                                syncLock.wait(SYNC_LOCK_WAIT_TIMEOUT_MS);
                            }
                        } else {
                            recordBatch = batchQueue.peek();
                            if (recordBatch != null) {
                                doWrite(recordBatch);
                                batchQueue.pop();
                            } else {
                                batchQueue.waitEmpty(configure.getPutRecordQueueTimeoutMs(), TimeUnit.MILLISECONDS);
                            }
                        }
                    } catch (ShardSealedException e) {
                        logger.warn("Shard status change, table: {}, topic: {}, shardId: {}",
                                tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId, e);
                        isShardClosed = true;
                        stop = true;
                    } catch (InterruptedException e) {
                        if (!stop) {
                            logger.error("ShardWriter is interrupted, topicName: {}, shardId: {}",
                                    tableMapping.getProjectName(), shardId, e);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("ShardWriter encounter an unexpected fail, table: {}, topic: {}, shardId: {}",
                        tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId, e);
                exception = e;
            }
            stop = true;
            logger.warn("ShardWriter stopped, table: {}, topic: {}, shardId: {}",
                    tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId);
        }

        private void doWrite(RecordBatch recordBatch) {
            if (recordBatch == null || recordBatch.isEmpty()) {
                return;
            }

            List<RecordEntry> records = recordBatch.getRecords();
            for (RecordEntry record : records) {
                record.setPartitionKey(null);
                record.setHashKey(null);
                record.setShardId(shardId);
            }

            long startTime = System.currentTimeMillis();
            String errorMessage = "unknown";
            PutRecordsResult putRecordsResult = null;

            int retryCount = 0;
            while (!stop) {
                try {
                    putRecordsResult = client.putRecords(tableMapping.getProjectName(), tableMapping.getTopicName(), records);
                } catch (DatahubClientException e) {
                    logger.error("Write DataHub failed. table: {}, topic: {}, shard: {}, recordNum: {}",
                            tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId, records.size(), e);
                    errorMessage = e.getErrorMessage();
                }

                if (putRecordsResult != null) {
                    if (putRecordsResult.getFailedRecordCount() == 0) {

                        if (configure.isReportMetric()) {
                            MetricHelper.instance().addPutTime(System.currentTimeMillis() - startTime);
                        }
                        logger.info("Write DataHub success, table: {}, topic: {}, shard: {}, recordNum: {}",
                                tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId, records.size());

                        PluginStatictics.addSendTimesInTx();
                        break;
                    } else {
                        errorMessage = putRecordsResult.getPutErrorEntries().get(0).getMessage();
                    }
                }

                if (configure.getRetryTimes() < 0 || retryCount < configure.getRetryTimes()) {
                    if (putRecordsResult != null && putRecordsResult.getFailedRecordCount() > 0) {
                        logger.error("Write DataHub failed. table: {}, project: {}, topic: {}, shard: {}, failedNum: {}, ErrorCode: {}, Message : {}",
                                tableMapping.getOracleFullTableName(), tableMapping.getProjectName(),
                                tableMapping.getTopicName(), shardId, putRecordsResult.getFailedRecordCount(),
                                putRecordsResult.getPutErrorEntries().get(0).getErrorcode(),
                                putRecordsResult.getPutErrorEntries().get(0).getMessage());

                        List<RecordEntry> failedRecords = putRecordsResult.getFailedRecords();

                        for (int i = 0; i < failedRecords.size(); ++i) {
                            RecordEntry entry = failedRecords.get(i);
                            PutErrorEntry errorEntry = putRecordsResult.getPutErrorEntries().get(i);

                            if ("MalformedRecord".equals(errorEntry.getErrorcode())) {
                                if (configure.isDirtyDataContinue()) {
                                    BadOperateWriter.write(entry, tableMapping.getOracleFullTableName(), tableMapping.getTopicName(),
                                            configure.getDirtyDataFile(), configure.getDirtyDataFileMaxSize(), errorEntry.getMessage());
                                } else {
                                    logger.error("Write DataHub failed, dirty data found, table: {}, topic: {}, shard: {}, record: {}",
                                            tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId, JsonHelper.beanToJson(entry));

                                    throw new MalformedRecordException("Write DataHub failed, found dirty data");
                                }
                            }

                            // reset shardId if need
                            if ("InvalidShardOperation".equals(errorEntry.getErrorcode())) {
                                isShardClosed = true;
                                throw new ShardSealedException(new DatahubClientException(errorEntry.getMessage()));
                            }
                        }
                    }

                    logger.warn("Write DataHub failed, retry count [{}], will retry after {} ms, table: {}, topic: {}, shard: {}",
                            retryCount, configure.getRetryIntervalMs(), tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId);

                    try {
                        Thread.sleep(configure.getRetryIntervalMs());
                    } catch (InterruptedException ignored) {
                    }
                } else {
                    // no retry or retry count exceed.
                    logger.warn("Write DataHub failed, retry count exceed, table: {}, topic: {}, shard: {}",
                            tableMapping.getOracleFullTableName(), tableMapping.getTopicName(), shardId);

                    if (configure.isDirtyDataContinue()) {
                        // put record failed for putRecord throw exception
                        if (putRecordsResult == null) {
                            for (RecordEntry entry : records) {
                                BadOperateWriter.write(entry, tableMapping.getOracleFullTableName(), tableMapping.getTopicName(),
                                        configure.getDirtyDataFile(), configure.getDirtyDataFileMaxSize(), errorMessage);
                            }
                        } else {
                            for (int i = 0; i < putRecordsResult.getFailedRecordCount(); ++i) {
                                BadOperateWriter.write(putRecordsResult.getFailedRecords().get(i), tableMapping.getOracleFullTableName(),
                                        tableMapping.getTopicName(), configure.getDirtyDataFile(), configure.getDirtyDataFileMaxSize(), errorMessage);
                            }
                        }
                    } else {
                        throw new DatahubClientException("Write DataHub failed, " + errorMessage);
                    }
                    break;
                }
                retryCount++;
            }
        }
    }
}
