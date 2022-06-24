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

import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.MetricHelper;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ShardWriter {
    private static final Logger logger = LoggerFactory.getLogger(ShardWriter.class);
    private static final Logger accessLogger = LoggerFactory.getLogger("datahubaccess");

    private static final int WAIT_TASK_TIMEOUT_MS = 5000;
    private static final List<String> RETRY_ERROR_CODE;

    static {
        RETRY_ERROR_CODE = new ArrayList<>();
        RETRY_ERROR_CODE.add("LimitExceeded");
        RETRY_ERROR_CODE.add("ShardNotReady");
        RETRY_ERROR_CODE.add("InternalServerError");
    }

    private Configure configure;
    private ExecutorService executor;

    private String oracleTableName;
    private String projectName;
    private String topicName;
    private String shardId;

    private RecordBatch recordBatch = new RecordBatch();
    private RecordBatchQueue batchQueue = null;
    private volatile Future currentTask = null;

    private Exception exception = null;
    private volatile boolean stop = false;

    ShardWriter(Configure configure, ExecutorService executor, String oracleTableName, String projectName, String topicName, String shardId) {
        this.configure = configure;
        this.executor = executor;

        this.oracleTableName = oracleTableName;
        this.projectName = projectName;
        this.topicName = topicName;
        this.shardId = shardId;
        this.batchQueue = new RecordBatchQueue(configure.getWriteRecordQueueSize());
    }

    public void start() {
        stop = false;
    }

    public void close() {
        stop = true;
    }

    public RecordBatchQueue getBatchQueue() {
        return batchQueue;
    }

    public RecordBatch getRecordBatch() {
        return recordBatch;
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

    public void writeRecord(RecordEntry recordEntry) {
        checkWriter();

        recordBatch.add(recordEntry);
        if (isReady(recordBatch)) {
            submit();
        }
    }

    private void checkWriter() {
        if (stop) {
            throw new ShardSealedException(new DatahubClientException("shard [" + shardId + "] already stopped"));
        }

        if (exception != null) {
            throw new RuntimeException(exception.getMessage());
        }
    }

    private boolean taskDone() {
        return currentTask == null || currentTask.isDone();
    }

    private void awaitTaskDone() {
        while (!taskDone()) {
            try {
                currentTask.get(WAIT_TASK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Wait write task finished error, project: {}, topic: {}", projectName, topicName, e);
            } catch (TimeoutException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Wait write task finished timeout,  project: {}, topic: {}", projectName, topicName);
                }
            }
        }

        checkWriter();
    }

    private boolean isReady(RecordBatch batch) {
        return batch.getRecords().size() >= configure.getBatchSize()
                || System.currentTimeMillis() - batch.getCreateTimestamp() >= configure.getBatchTimeoutMs();
    }

    private boolean submit() {
        if (!recordBatch.isEmpty()) {
            try {
                boolean success = false;
                do {
                    success = batchQueue.offer(recordBatch, configure.getWriteRecordQueueTimeoutMs(), TimeUnit.MILLISECONDS);
                } while (!success);
                recordBatch = new RecordBatch();
            } catch (InterruptedException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Offer record batch failed, table: {}, topic: {}", oracleTableName, topicName, e);
                }
            }
        }

        boolean ret = true;
        if (!batchQueue.isEmpty() && taskDone()) {
            WriteTask task = new WriteTask();
            try {
                currentTask = executor.submit(task);
            } catch (RejectedExecutionException e) {
                logger.warn("Submit write task failed, table: {}, topic: {}, shard: {}",
                        oracleTableName, topicName, shardId, e);
                ret = false;
            }
        }
        return ret;
    }

    private class WriteTask implements Runnable {
        public WriteTask() {
        }

        @Override
        public void run() {
            try {
                RecordBatch recordBatch = null;
                while (!stop) {
                    try {
                        recordBatch = batchQueue.peek();
                        if (recordBatch != null) {
                            doWrite(recordBatch);
                            batchQueue.pop();
                        } else {
                            break;
                        }
                    } catch (ShardSealedException e) {
                        logger.warn("Shard status change, table: {}, topic: {}, shard: {}",
                                oracleTableName, topicName, shardId, e);
                        stop = true;
                    }
                }
            } catch (Exception e) {
                logger.error("ShardWriter encounter an unexpected fail, table: {}, topic: {}, shard: {}",
                        oracleTableName, topicName, shardId, e);
                exception = e;
            }
        }

        private void doWrite(RecordBatch recordBatch) {
            if (recordBatch == null || recordBatch.isEmpty()) {
                return;
            }

            List<RecordEntry> records = recordBatch.getRecords();
            int retryCount = 0;
            while (!stop) {
                long startTime = System.currentTimeMillis();
                try {
                    PutRecordsByShardResult result = ClientHelper.instance().getClient()
                            .putRecordsByShard(projectName, topicName, shardId, records);

                    long rt = System.currentTimeMillis() - startTime;
                    if (configure.isReportMetric()) {
                        MetricHelper.instance().addSend(rt);
                    }
                    if (configure.isRecordAccess()) {
                        accessLogger.info("{}/{}/{},{},Send,OK,{},{},{}", projectName, topicName,
                                shardId, oracleTableName, rt, records.size(), result.getRequestId());
                    }
                    PluginStatictics.addSendTimesInTx();
                    break;
                } catch (DatahubClientException e) {
                    long rt = System.currentTimeMillis() - startTime;
                    logger.error("Write DataHub failed. table: {}, topic: {}, shard: {}, recordNum: {}",
                            oracleTableName, topicName, shardId, records.size(), e);
                    if (configure.isRecordAccess()) {
                        accessLogger.info("{}/{}/{},{},Send,{},{},{},{}", projectName, topicName,
                                shardId, oracleTableName, e.getErrorCode(),rt, records.size(), e.getRequestId());
                    }

                    if (e instanceof ShardSealedException) {
                        throw e;
                    } else if (e instanceof MalformedRecordException) {
                        if (!configure.isDirtyDataContinue()) {
                            throw e;
                        }
                        for (RecordEntry entry : records) {
                            BadOperateWriter.write(entry, oracleTableName, topicName, configure.getDirtyDataFile(),
                                    configure.getDirtyDataFileMaxSize(), e.getErrorMessage());
                        }
                        break;
                    } else {
                        if (!RETRY_ERROR_CODE.contains(e.getErrorCode())
                                || (configure.getRetryTimes() >= 0
                                && retryCount >= configure.getRetryTimes())) {
                            throw e;
                        }

                        logger.warn("Write DataHub failed, retry count [{}], will retry after {} ms, table: {}, topic: {}, shard: {}, retryRecord: {}",
                                retryCount, configure.getRetryIntervalMs(), oracleTableName, topicName, shardId, records.size());
                        try {
                            Thread.sleep(configure.getRetryIntervalMs());
                        } catch (InterruptedException ignored) {
                        }
                        retryCount++;
                    }
                }
            }
        }
    }
}
