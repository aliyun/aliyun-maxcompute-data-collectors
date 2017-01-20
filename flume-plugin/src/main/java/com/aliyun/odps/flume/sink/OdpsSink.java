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

package com.aliyun.odps.flume.sink;

import com.aliyun.datahub.flume.sink.serializer.OdpsDelimitedTextSerializer;
import com.aliyun.datahub.flume.sink.serializer.OdpsEventSerializer;
import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.flume.sink.dataobject.OdpsRowDO;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.mina.core.RuntimeIoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * A simple sink which reads events from a channel and writes them to ODPS.
 */
public class OdpsSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(OdpsSink.class);

    private static final String DEFAULT_ODPS_END_POINT = "http://service.odps.aliyun.com/api";
    private static final String DEFAULT_ODPS_DATAHUB_END_POINT = "http://dh.odps.aliyun.com";
    private static final String DEFAULT_ODPS_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static final int DEFAULT_ODPS_BATCHSIZE = 100;
    private static final int DEFAULT_SHARD_NUMBER = 1;

    /**
     * Default timeout for stop waiting shard load, unit is second.
     */
    private static final int DEFAULT_LOAD_SHARD_TIME = 60;
    private static final boolean DEFAULT_AUTO_CREATE_PARTITION = true;
    private static final boolean DEFAULT_NEED_ROUNDING = false;


    private String accessId;
    private String accessKey;
    private String odpsEndPoint;
    private String datahubEndPoint;
    private String project;
    private String table;
    private List<String> partitionValues;
    private boolean autoCreatePartition;
    private String dateFormat;

    private StreamClient streamClient;
    private StreamWriter[] streamWriters;
    private OdpsWriter odpsWriter;
    private List<String> partitionFields;
    private Table odpsTable;
    private Map<String, Boolean> partitionMap;

    private String serializerType;
    private OdpsEventSerializer serializer;

    private int batchSize;
    private int shardNumber;
    private int maxLoadShardTime;

    private boolean useLocalTime;
    private TimeZone timeZone;
    private boolean needRounding;
    private int roundUnit;
    private Integer roundValue;

    private SinkCounter sinkCounter;

    @Override
    public void configure(Context context) {
        accessId = Preconditions.checkNotNull(context.getString(ConfigConstants.ACCESS_ID), "%s config setting is not" +
                " specified for sink %s", ConfigConstants.ACCESS_ID, getName());
        accessKey = Preconditions.checkNotNull(context.getString(ConfigConstants.ACCESS_KEY), "%s config setting is " +
                "not specified for sink %s", ConfigConstants.ACCESS_KEY, getName());
        odpsEndPoint = context.getString(ConfigConstants.ODPS_END_POINT, DEFAULT_ODPS_END_POINT);
        datahubEndPoint = context.getString(ConfigConstants.ODPS_DATAHUB_END_POINT, DEFAULT_ODPS_DATAHUB_END_POINT);
        project = Preconditions.checkNotNull(context.getString(ConfigConstants.ODPS_PROJECT), "%s config setting is " +
                "not specified for sink %s", ConfigConstants.ODPS_PROJECT, getName());
        table = Preconditions.checkNotNull(context.getString(ConfigConstants.ODPS_TABLE), "%s config setting is not " +
                "specified for sink %s", ConfigConstants.ODPS_TABLE, getName());
        autoCreatePartition = context.getBoolean(ConfigConstants.AUTO_CREATE_PARTITION, DEFAULT_AUTO_CREATE_PARTITION);
        dateFormat = context.getString(ConfigConstants.DATE_FORMAT, DEFAULT_ODPS_DATE_FORMAT);

        shardNumber = context.getInteger(ConfigConstants.SHARD_NUMBER, DEFAULT_SHARD_NUMBER);
        if (shardNumber < 1) {
            logger.warn("{}.shardNumber must be greater than 0. Defaulting to {}", getName(), DEFAULT_SHARD_NUMBER);
            shardNumber = DEFAULT_SHARD_NUMBER;
        }

        String partitions = context.getString(ConfigConstants.ODPS_PARTITION);
        if (partitions != null) {
            partitionValues = Arrays.asList(partitions.split(","));
        }

        serializerType = Preconditions.checkNotNull(context.getString(ConfigConstants.SERIALIZER), "%s config setting" +
                " is not specified for sink %s", ConfigConstants.SERIALIZER, getName());
        serializer = this.createSerializer(serializerType);
        Context serializerContext = new Context();
        serializerContext.putAll(context.getSubProperties(ConfigConstants.SERIALIZER_PREFIX));
        serializer.configure(serializerContext);

        batchSize = context.getInteger(ConfigConstants.BATCH_SIZE, DEFAULT_ODPS_BATCHSIZE);
        if (batchSize < 0) {
            logger.warn("{}.batchSize must be positive number. Defaulting to {}", getName(), DEFAULT_ODPS_BATCHSIZE);
            batchSize = DEFAULT_ODPS_BATCHSIZE;
        }

        maxLoadShardTime = context.getInteger(ConfigConstants.MAX_LOAD_SHARD_TIME, DEFAULT_LOAD_SHARD_TIME);
        if (maxLoadShardTime <= 0) {
            logger.warn("{}.maxLoadShardTime must be positive number. Defaulting to {}", getName(),
                    DEFAULT_LOAD_SHARD_TIME);
            maxLoadShardTime = DEFAULT_LOAD_SHARD_TIME;
        }

        useLocalTime = context.getBoolean(ConfigConstants.USE_LOCAL_TIME_STAMP, false);
        String tzName = context.getString(ConfigConstants.TIME_ZONE);
        timeZone = (StringUtils.isEmpty(tzName)) ? null : TimeZone.getTimeZone(tzName);
        needRounding = context.getBoolean(ConfigConstants.NEED_ROUNDING, DEFAULT_NEED_ROUNDING);
        String unit = context.getString(ConfigConstants.ROUND_UNIT, ConfigConstants.MINUTE);
        if (unit.equalsIgnoreCase(ConfigConstants.HOUR)) {
            this.roundUnit = Calendar.HOUR_OF_DAY;
        } else if (unit.equalsIgnoreCase(ConfigConstants.MINUTE)) {
            this.roundUnit = Calendar.MINUTE;
        } else if (unit.equalsIgnoreCase(ConfigConstants.SECOND)) {
            this.roundUnit = Calendar.SECOND;
        } else {
            logger.warn(getName() + ". Rounding unit is not valid, please set one of " +
                    "minute, hour or second. Rounding will be disabled");
            needRounding = false;
        }
        this.roundValue = context.getInteger(ConfigConstants.ROUND_VALUE, 1);
        if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
            Preconditions.checkArgument(roundValue > 0 && roundValue <= 60, "Round value must be > 0 and <= 60");
        } else if (roundUnit == Calendar.HOUR_OF_DAY) {
            Preconditions.checkArgument(roundValue > 0 && roundValue <= 24, "Round value must be > 0 and <= 24");
        }

        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
    }

    private OdpsEventSerializer createSerializer(String serializerType) {
        if (serializerType.compareToIgnoreCase(OdpsDelimitedTextSerializer.ALIAS) == 0 || serializerType.compareTo
                (OdpsDelimitedTextSerializer.class.getName()) == 0) {
            return new OdpsDelimitedTextSerializer();
        }

        try {
            return (OdpsEventSerializer) Class.forName(serializerType).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to instantiate serializer: " + serializerType + " on sink: " +
                    getName(), e);
        }
    }

    @Override
    public void start() {
        logger.info("Odps Sink {}: starting...", getName());
        super.start();
        // Sleep a random time (<= 60s)
        try {
            Thread.sleep((new Random()).nextInt(60000));
        } catch (InterruptedException e) {
            // DO NOTHING
        }
        // Initial datahub writers
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setUserAgent("odps-flume-1.0.0");
        odps.setDefaultProject(project);
        odps.setEndpoint(odpsEndPoint);
        odpsTable = odps.tables().get(table);
        if (partitionValues != null) {
            List<Column> partitionCols = odpsTable.getSchema().getPartitionColumns();
            if (partitionCols.size() != partitionValues.size()) {
                logger.error("Odps partition fields number not equals input partition values number");
                throw new FlumeException("Odps partition fields number not equals input partition values number");
            }
            partitionFields = Lists.newArrayList();
            for (Column partCol : partitionCols) {
                partitionFields.add(partCol.getName());
            }
            buildPartitionMap();
        }
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(datahubEndPoint);

        try {
            streamClient = tunnel.createStreamClient(project, table);
            streamClient.loadShard(shardNumber);
        } catch (TunnelException e) {
            logger.error("Error initializing stream client from tunnel. Project: " + project + ", Table: " + table +
                    ".", e);
            throw new FlumeException("Error initializing stream client from tunnel. Project: " + project + ", Table: " +
                    "" + table + ".", e);
        }

        try {
            buildStreamWriters();
        } catch (Exception e) {
            logger.error("Error build ODPS stream writers.", e);
            throw new FlumeException("Error build ODPS stream writers.", e);
        }
        try {
            odpsWriter = serializer.createOdpsWriter(odpsTable, streamWriters, dateFormat);
        } catch (UnsupportedEncodingException e) {
            logger.error("Error create ODPS stream writers.", e);
            throw new FlumeException("create odps writer failed!");
        }
        sinkCounter.start();
        logger.info("Odps Sink {}: started", getName());
    }

    private void buildPartitionMap() {
        partitionMap = Maps.newHashMap();
        for (Partition partition : odpsTable.getPartitions()) {
            partitionMap.put(partition.getPartitionSpec().toString(), true);
        }
    }

    private void buildStreamWriters() throws IOException, TunnelException {
        final StreamClient.ShardState finish = StreamClient.ShardState.LOADED;
        long now = System.currentTimeMillis();
        long endTime = now + maxLoadShardTime * 1000;
        List<Long> shardIDList = null;
        while (now < endTime) {
            HashMap<Long, StreamClient.ShardState> shardStatus = streamClient.getShardStatus();
            shardIDList = new ArrayList<Long>();
            Set<Long> keys = shardStatus.keySet();
            Iterator<Long> iter = keys.iterator();
            while (iter.hasNext()) {
                Long key = iter.next();
                StreamClient.ShardState value = shardStatus.get(key);
                if (value.equals(finish)) {
                    shardIDList.add(key);
                }
            }
            now = System.currentTimeMillis();
            if (shardIDList.size() == shardNumber) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // DO NOTHING...
            }
        }
        if (shardIDList != null && shardIDList.size() > 0) {
            this.streamWriters = new StreamWriter[shardIDList.size()];
            for (int i = 0; i < shardIDList.size(); i++) {
                this.streamWriters[i] = streamClient.openStreamWriter(shardIDList.get(i));
            }
        } else {
            throw new RuntimeException("Odps Sink " + getName() + " buildStreamWriter() error, have not loaded shards" +
                    ".");
        }
    }

    @Override
    public void stop() {
        sinkCounter.stop();
        super.stop();
        logger.info("Odps Sink {}: stopped", getName());
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.debug("Sink {} processing...", getName());
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();
            List<OdpsRowDO> rowList = Lists.newLinkedList();
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                serializer.initialize(event);
                OdpsRowDO odpsRowDO = new OdpsRowDO();
                odpsRowDO.setRowMap(serializer.getRow());
                String partitionSpec = buildPartitionSpec(partitionValues, event.getHeaders(), timeZone,
                        needRounding, roundUnit, roundValue, useLocalTime);
                odpsRowDO.setPartitionSpec(partitionSpec);
                rowList.add(odpsRowDO);
            }
            sinkCounter.addToEventDrainAttemptCount(rowList.size());
            if (rowList.size() == 0) {
                sinkCounter.incrementBatchEmptyCount();
                logger.debug("No events in channel {}", getChannel().getName());
                status = Status.BACKOFF;
            } else {
                // Write batch to ODPS table
                odpsWriter.write(rowList);
                logger.info("Write success. Sink: {}, Event count: {}", getName(), rowList.size());
                if (batchSize == rowList.size()) {
                    sinkCounter.incrementBatchCompleteCount();
                } else {
                    sinkCounter.incrementBatchUnderflowCount();
                }
                sinkCounter.addToEventDrainSuccessCount(rowList.size());
            }
            transaction.commit();
        } catch (Throwable t) {
            transaction.rollback();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ChannelException) {
                logger.error("Odps Sink " + getName() + ": Unable to get event from channel " + channel.getName() + "" +
                        ". Exception follows.", t);
                status = Status.BACKOFF;
            } else {
                throw new EventDeliveryException("Failed to send events", t);
            }
        } finally {
            transaction.close();
        }
        return status;
    }

    private String buildPartitionSpec(List<String> partitionValues, Map<String, String> headers, TimeZone timeZone,
                                      boolean needRounding, int roundUnit, Integer roundValue, boolean useLocalTime)
            throws OdpsException {
        if (partitionValues == null || partitionValues.size() == 0) {
            return StringUtils.EMPTY;
        }
        if (partitionValues.size() != partitionFields.size()) {
            throw new RuntimeException("Odps partition fields number not equals input partition values number");
        }
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (int i = 0; i < partitionValues.size(); i++) {
            String realPartVal = BucketPath.escapeString(partitionValues.get(i), headers, timeZone, needRounding,
                    roundUnit, roundValue, useLocalTime);
            sb.append(sep).append(partitionFields.get(i)).append("='").append(realPartVal).append("'");
            sep = ",";
        }
        String partitionSpec = sb.toString();
        if (autoCreatePartition && !partitionMap.containsKey(partitionSpec)) {
            odpsTable.createPartition(new PartitionSpec(partitionSpec), true);
            partitionMap.put(partitionSpec, true);
        }
        return partitionSpec;
    }

}
