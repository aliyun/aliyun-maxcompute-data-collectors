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

import com.aliyun.datahub.flume.sink.serializer.OdpsDelimitedTextSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsRegexEventSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsEventSerializer;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * A simple sink which reads events from a channel and writes them to Datahub.
 */
public class DatahubSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(DatahubSink.class);

    private Configure configure;
    private OdpsEventSerializer serializer;
    private SinkCounter sinkCounter;

    private DatahubWriter datahubWriter;

    @Override public void configure(Context context) {
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
        String dateFormat =
            context.getString(DatahubConfigConstants.DATE_FORMAT, Configure.DEFAULT_DATE_FORMAT);
        configure.setDateFormat(dateFormat);

        String shardId = context.getString(DatahubConfigConstants.DATAHUB_SHARD_ID);
        configure.setShardId(shardId);

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

        String shardColumns = context.getString(DatahubConfigConstants.SHARD_COLUMNS, "");
        configure.setShardColumnNames(StringUtils.split(shardColumns, ","));
        String dateformatColumns =
            context.getString(DatahubConfigConstants.DATE_FORMAT_COLUMNS, "");
        configure.setDateformatColumnNames(StringUtils.split(dateformatColumns, ","));

        int batchSize = context
            .getInteger(DatahubConfigConstants.BATCH_SIZE, Configure.DEFAULT_DATAHUB_BATCHSIZE);
        if (batchSize < 0) {
            logger.warn("{}.batchSize must be positive number. Defaulting to {}", getName(),
                Configure.DEFAULT_DATAHUB_BATCHSIZE);
            batchSize = Configure.DEFAULT_DATAHUB_BATCHSIZE;
        }
        configure.setBatchSize(batchSize);

        int retryTimes =
            context.getInteger(DatahubConfigConstants.RETRY_TIMES, Configure.DEFAULT_RETRY_TIMES);
        configure.setRetryTimes(retryTimes);
        int retryInterval = context
            .getInteger(DatahubConfigConstants.RETRY_INTERVAL, Configure.DEFAULT_RETRY_INTERVAL);
        configure.setRetryInterval(retryInterval);

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
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

    @Override public void start() {
        logger.info("Odps Sink {}: starting...", getName());
        super.start();
        // Sleep a random time (<= 5s)
        try {
            Thread.sleep((new Random()).nextInt(5000));
        } catch (InterruptedException e) {
            // DO NOTHING
        }

        // Initial datahub writer
        datahubWriter = new DatahubWriter(configure, sinkCounter);
        logger.info("Init DatahubWriter success");

        sinkCounter.start();
        logger.info("Odps Sink {}: started", getName());
    }

    @Override public void stop() {
        sinkCounter.stop();
        super.stop();
        logger.info("Odps Sink {}: stopped", getName());
    }

    @Override public Status process() throws EventDeliveryException {
        logger.debug("Sink {} processing...", getName());
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        logger.debug("batch process size: " + configure.getBatchSize());
        try {
            transaction.begin();
            for (int i = 0; i < configure.getBatchSize(); i++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                serializer.initialize(event);
                Map<String,String> rowMap = serializer.getRow();
                if (!rowMap.isEmpty()) {
                    datahubWriter.addRecord(rowMap);
                }
            }
            int recordSize = datahubWriter.getRecordSize();
            sinkCounter.addToEventDrainAttemptCount(recordSize);
            if (recordSize == 0) {
                sinkCounter.incrementBatchEmptyCount();
                logger.debug("No events in channel {}", getChannel().getName());
                status = Status.BACKOFF;
            } else {
                // Write batch to Datahub
                datahubWriter.writeToHub();

                logger.info("Write success. Sink: {}, Event count: {}", getName(), recordSize);
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
                logger.error("Odps Sink " + getName() + ": Unable to get event from channel " +
                    channel.getName() + "" + ". Exception follows.", t);
                status = Status.BACKOFF;
            } else {
                throw new EventDeliveryException("Failed to send events", t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

}
