package com.aliyun.datahub.flume.sink;


import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.flume.sink.serializer.OdpsDelimitedTextSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsEventSerializer;
import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatahubSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory.getLogger(DatahubSource.class);

    private DatahubReader datahubReader;
    private OdpsEventSerializer serializer;
    private Configure configure;
    private SourceCounter sourceCounter;

    @Override
    public void configure(Context context) {
        configure = new Configure();

        String accessId = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_ACCESS_ID),
                        "%s config setting is not" + " specified for source %s",
                        DatahubConfigConstants.DATAHUB_ACCESS_ID, getName());
        configure.setAccessId(accessId);

        String accessKey = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_ACCESS_KEY),
                        "%s config setting is " + "not specified for source %s",
                        DatahubConfigConstants.DATAHUB_ACCESS_KEY, getName());
        configure.setAccessKey(accessKey);

        String endPoint = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_END_POINT),
                        "%s config setting is " + "not specified for sink %s",
                        DatahubConfigConstants.DATAHUB_END_POINT, getName());
        configure.setEndPoint(endPoint);

        String projectName = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_PROJECT),
                        "%s config setting is not " + "specified for source %s",
                        DatahubConfigConstants.DATAHUB_PROJECT, getName());
        configure.setProject(projectName);

        String topic = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_TOPIC),
                        "%s config setting is not " + "specified for source %s",
                        DatahubConfigConstants.DATAHUB_TOPIC, getName());
        configure.setTopic(topic);

        String subId = Preconditions
                .checkNotNull(context.getString(DatahubConfigConstants.DATAHUB_SUB_ID),
                        "%s config setting is not " + "specified for source %s",
                        DatahubConfigConstants.DATAHUB_SUB_ID, getName());
        configure.setSubId(subId);

        String startTime = context.getString(DatahubConfigConstants.DATAHUB_START_TIME);
        configure.setStartTimestamp(startTime);

        String shardIds = context.getString(DatahubConfigConstants.DATAHUB_SHARD_IDS);
        if (shardIds != null) {
            List<String> ids = Arrays.asList(shardIds.split(","));
            configure.setShardIds(ids);
        }

        String compressType = context.getString(DatahubConfigConstants.DATAHUB_COMPRESS_TYPE);
        configure.setCompressType(compressType);

        boolean enablePb = context.getBoolean(DatahubConfigConstants.DATAHUB_ENABLE_PB, Configure.DEFAULT_ENABLE_PB);
        configure.setEnablePb(enablePb);

        int batchSize = context.getInteger(DatahubConfigConstants.BATCH_SIZE, Configure.DEFAULT_DATAHUB_BATCHSIZE);
        configure.setBatchSize(batchSize);

        int batchTimeout = context.getInteger(DatahubConfigConstants.BATCH_TIMEOUT, Configure.DEFAULT_DATAHUB_BATCHTIMEOUT);
        configure.setBatchTimeout(batchTimeout);

        int retryTimes = context.getInteger(DatahubConfigConstants.RETRY_TIMES, Configure.DEFAULT_RETRY_TIMES);
        configure.setRetryTimes(retryTimes);

        boolean autoCommit = context.getBoolean(DatahubConfigConstants.AUTO_COMMIT, Configure.DEFAULT_AUTO_COMMIT);
        configure.setAutoCommit(autoCommit);

        int commitInterval = context.getInteger(DatahubConfigConstants.OFFSET_COMMIT_INTERVAL, Configure.DEFAULT_OFFSET_COMMIT_INTERVAL);
        configure.setOffsetCommitInterval(commitInterval);

        int sessionTimeout = context.getInteger(DatahubConfigConstants.SESSION_TIMEOUT, Configure.DEFAULT_SESSION_TIMEOUT);
        configure.setSessionTimeout(sessionTimeout);

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

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        if (logger.isInfoEnabled())
        {
            logger.info(configure.sourcetoString());
        }
    }

    private OdpsEventSerializer createSerializer(String serializerType) {
        if (serializerType.compareToIgnoreCase(OdpsDelimitedTextSerializer.ALIAS) == 0
                || serializerType.compareTo(OdpsDelimitedTextSerializer.class.getName()) == 0) {
            return new OdpsDelimitedTextSerializer();
        }
        throw new IllegalArgumentException("DataHub source not support serializer " + serializerType);
    }

    @Override
    public void start() {
        super.start();
        sourceCounter.start();
        if (datahubReader == null) {
            datahubReader = new DatahubReader(configure);
        }
        logger.info("DataHub Source {}: started", getName());
    }

    @Override
    public void stop() {
        datahubReader.close();
        sourceCounter.stop();
        super.stop();
        logger.info("DataHub Source {}: stopped", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        long threadId = Thread.currentThread().getId();
        logger.debug("[Thread " + threadId + "] " + "Source {} processing...", getName());
        List<Event> eventList = new ArrayList<Event>();
        Status status = Status.READY;

        long endTime = System.currentTimeMillis() + configure.getBatchTimeout() * 1000;
        int currentSize = 0;

        try {
            while (true) {
                RecordEntry entry = datahubReader.read();
                if (entry != null) {
                    Event e = serializer.getEvent(entry);
                    eventList.add(e);
                    currentSize++;
                    sourceCounter.incrementEventReceivedCount();
                }
                if (currentSize >= configure.getBatchSize()) {
                    logger.debug("[Thread {}] BatchSize finished.", threadId);
                    break;
                } else if (System.currentTimeMillis() > endTime) {
                    logger.debug("[Thread {}] Batch timeout.", threadId);
                    break;
                }
            }

            if (!eventList.isEmpty()) {
                getChannelProcessor().processEventBatch(eventList);
                sourceCounter.addToEventAcceptedCount(eventList.size());
                logger.info("[Thread {}] put {} event to channel successful.", threadId, eventList.size());
            } else {
                logger.debug("[Thread {}] DataHub no data now.", threadId);
            }
        } catch (Throwable t) {
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ChannelException) {
                logger.error("[Thread {}] DataHub source {}: write to a required channel fails. Exception follows.", threadId, getName(), t);
            } else {
                logger.error("[Thread {}] ", threadId, t);
                throw new EventDeliveryException("Failed to take events", t);
            }
        }
        return status;
    }
}