/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.datahub.flume.sink;

import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.datahub.flume.sink.serializer.OdpsDelimitedTextSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsEventSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsRegexEventSerializer;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A simple sink which reads events from a channel and writes them to Datahub.
 */
public class DatahubSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(DatahubSink.class);

    private Configure configure;
    private OdpsEventSerializer serializer;
    private SinkCounter sinkCounter;

    private DatahubWriter datahubWriter;

    @Override
    public void configure(Context context) {
        //threadId = Thread.currentThread().getId();

        configure = new Configure();

        String accessId = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_ACCESS_ID),
                        "%s config setting is not" + " specified for sink %s",
                        DatahubConfigConstants.DATAHUB_ACCESS_ID, getName());

        configure.setAccessId(accessId);
        String accessKey = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_ACCESS_KEY),
                        "%s config setting is " + "not specified for sink %s",
                        DatahubConfigConstants.DATAHUB_ACCESS_KEY, getName());

        configure.setAccessKey(accessKey);
        String endPoint = context.getString(DatahubConfigConstants.DATAHUB_END_POINT,
                Configure.DEFAULT_DATAHUB_END_POINT);
        configure.setEndPoint(endPoint);

        String projectName = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_PROJECT),
                        "%s config setting is not " + "specified for sink %s",
                        DatahubConfigConstants.DATAHUB_PROJECT, getName());
        configure.setProject(projectName);

        String topic = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_TOPIC),
                        "%s config setting is not " + "specified for sink %s",
                        DatahubConfigConstants.DATAHUB_TOPIC, getName());
        configure.setTopic(topic);

        String shardIds = context.getString(DatahubConfigConstants.DATAHUB_SHARD_IDS);
        if (shardIds != null && !shardIds.isEmpty()) {
            List<String> ids = Arrays.asList(shardIds.split(","));
            configure.setShardIds(ids);
        }

        String compressType = context.getString(DatahubConfigConstants.DATAHUB_COMPRESS_TYPE);
        configure.setCompressType(compressType);

        boolean enablePb = context.getBoolean(DatahubConfigConstants.DATAHUB_ENABLE_PB, Configure.DEFAULT_ENABLE_PB);
        configure.setEnablePb(enablePb);

        String serializerType = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.SERIALIZER),
                        "%s config setting" + " is not specified for sink %s",
                        DatahubConfigConstants.SERIALIZER, getName());
        configure.setSerializerType(serializerType);
        serializer = this.createSerializer(serializerType);

        Context serializerContext = new Context();
        serializerContext
                .putAll(context.getSubProperties(DatahubConfigConstants.SERIALIZER_PREFIX));
        serializer.configure(serializerContext);
        configure.setInputColumnNames(serializer.getInputColumnNames());

        int batchSize = context.getInteger(DatahubConfigConstants.BATCH_SIZE, Configure.DEFAULT_DATAHUB_BATCHSIZE);
        if (batchSize < 0) {
            logger.warn("{}.batchSize must be positive number. Defaulting to {}", getName(),
                    Configure.DEFAULT_DATAHUB_BATCHSIZE);
            batchSize = Configure.DEFAULT_DATAHUB_BATCHSIZE;
        }
        configure.setBatchSize(batchSize);

        int maxBufferSize = context.getInteger(DatahubConfigConstants.MAX_Buffer_SIZE, Configure.DEFAULT_DATAHUB_MAX_BUFFERSIZE);
        configure.setMaxBufferSize(maxBufferSize);


        int batchTimeout = context.getInteger(DatahubConfigConstants.BATCH_TIMEOUT, Configure.DEFAULT_DATAHUB_BATCHTIMEOUT);
        configure.setBatchTimeout(batchTimeout);

        int retryTimes = context.getInteger(DatahubConfigConstants.RETRY_TIMES, Configure.DEFAULT_RETRY_TIMES);
        configure.setRetryTimes(retryTimes);

        int retryInterval = context.getInteger(DatahubConfigConstants.RETRY_INTERVAL, Configure.DEFAULT_RETRY_INTERVAL);
        configure.setRetryInterval(retryInterval);

        boolean dirtyDataContinue = context.getBoolean(DatahubConfigConstants.Dirty_DATA_CONTINUE, Configure.DEFAULT_DIRTY_DATA_CONTINUE);
        configure.setDirtyDataContinue(dirtyDataContinue);

        String dirtyDataFile = context.getString(DatahubConfigConstants.Dirty_DATA_FILE, Configure.DEFAULT_DIRTY_DATA_FILE);
        configure.setDirtyDataFile(dirtyDataFile);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        // Initial datahub writer
        if (datahubWriter == null) {
            try {
                datahubWriter = new DatahubWriter(configure);
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        logger.info("Init DatahubWriter success");
        logger.debug(configure.toString());
    }

    private OdpsEventSerializer createSerializer(String serializerType) {
        if (serializerType.compareToIgnoreCase(OdpsDelimitedTextSerializer.ALIAS) == 0
                || serializerType.compareTo(OdpsDelimitedTextSerializer.class.getName()) == 0) {
            return new OdpsDelimitedTextSerializer();
        } else if (serializerType.compareToIgnoreCase(OdpsRegexEventSerializer.ALIAS) == 0
                || serializerType.compareTo(OdpsRegexEventSerializer.class.getName()) == 0) {
            return new OdpsRegexEventSerializer();
        }

        try {
            return (OdpsEventSerializer) Class.forName(serializerType).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Unable to instantiate serializer: " + serializerType + " on sink: " +
                            getName(), e);
        }
    }

    @Override
    public void start() {
        super.start();
        // Sleep a random time (<= 5s)
        try {
            Thread.sleep((new Random()).nextInt(5000));
        } catch (InterruptedException e) {
            // DO NOTHING
        }
        sinkCounter.start();
        logger.info("Datahub Sink {}: started", getName());
    }

    @Override
    public void stop() {
        sinkCounter.stop();
        super.stop();
        try {
            datahubWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("close dirtyFile failed. ", e);
        }
        logger.info("Datahub Sink {}: stopped", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        long threadId = Thread.currentThread().getId();
        logger.debug("[Thread " + threadId + "] " + "Sink {} processing...", getName());
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        String shardId = datahubWriter.getActiveShardId();

        try {
            List<RecordEntry> recordEntries = new ArrayList<RecordEntry>();
            long endTime = System.currentTimeMillis() + configure.getBatchTimeout() * 1000;
            int currentSize = 0;
            int buffSize = 0;
            while (true) {
                Event event = channel.take();
                if (event == null) {
                    Thread.sleep(1000);
                } else {
                    serializer.initialize(event);
                    Map<String, String> rowMap = serializer.getRow();
                    if (!rowMap.isEmpty()) {
                        TupleRecordData data = datahubWriter.buildRecord(rowMap);
                        if (data != null) {
                            buffSize += event.getBody().length;
                            RecordEntry entry = new RecordEntry();
                            entry.setRecordData(data);
                            entry.setShardId(shardId);
                            recordEntries.add(entry);
                            currentSize++;
                        }
                    } else {
                        datahubWriter.handleDirtyData(serializer.getRawBody());
                    }
                }

                if (currentSize >= configure.getBatchSize()) {
                    logger.debug("[Thread {}] BatchSize finished.", threadId);
                    break;
                } else if (System.currentTimeMillis() > endTime) {
                    logger.debug("[Thread {}] Batch timeout.", threadId);
                    break;
                }

                if (buffSize > configure.getMaxBufferSize()) {
                    logger.debug("[Thread {}] " +
                            "Buffer size exceed maxBufferSize {}.", threadId, configure.getMaxBufferSize());
                    break;
                }
            }

            int recordSize = recordEntries.size();
            sinkCounter.addToEventDrainAttemptCount(recordSize);
            if (recordSize == 0) {
                sinkCounter.incrementBatchEmptyCount();
                logger.debug("[Thread {}] No events in channel {}.", threadId, getChannel().getName());
                status = Status.BACKOFF;
            } else {
                logger.debug("[Thread " + threadId + "] Record batch size is {}, buffer size is {} bytes, start sink to DataHub...", recordEntries.size(), buffSize);

                datahubWriter.writeRecords(recordEntries);

                if (configure.getBatchSize() == recordSize) {
                    sinkCounter.incrementBatchCompleteCount();
                } else {
                    sinkCounter.incrementBatchUnderflowCount();
                }
                sinkCounter.addToEventDrainSuccessCount(recordSize);
            }
            transaction.commit();
        } catch (Throwable t) {
            transaction.rollback();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ChannelException) {
                logger.error("[Thread " + threadId + "] DataHub Sink {}: Unable to get event from channel {}. Exception follows.",  channel.getName(), t);
                status = Status.BACKOFF;
            } else {
                logger.error("[Thread {}] ", threadId, t);
                throw new EventDeliveryException("Failed to take events", t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }
}
