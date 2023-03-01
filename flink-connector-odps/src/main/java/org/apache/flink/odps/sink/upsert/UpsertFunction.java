/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.partition.PartitionComputer;
import org.apache.flink.odps.sink.partition.TablePartitionAssigner;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.sink.table.UpsertWriter;
import org.apache.flink.odps.sink.utils.DataBucket;
import org.apache.flink.odps.sink.utils.DataItem;
import org.apache.flink.odps.sink.utils.RecordOperationType;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.util.RowDataToOdpsConverters;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class UpsertFunction extends AbstractUpsertFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(UpsertFunction.class);

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private final OdpsWriteOptions writeOptions;
    private final PartitionAssigner<RowData> partitionAssigner;
    private final boolean isDynamicPartition;
    private final boolean supportsGrouping;
    private final DataSchema dataSchema;
    private final boolean isPartitioned;
    private String partition;

    private transient EnvironmentSettings settings;
    private transient Odps odps;
    private transient MailboxExecutor executor;

    private transient RowDataToOdpsConverters.RowDataToOdpsRecordConverter converter;
    private transient Map<String, DataBucket> buckets;
    private transient Map<String, TableUpsertSessionImpl> odpsUpsertSessionMap;
    private transient Map<String, UpsertWriter<ArrayRecord>> odpsUpsertWriterMap;
    private transient List<WriterStatus> statuses;
    private transient boolean commitConfirming;

    public UpsertFunction(
            Configuration config,
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            DataSchema schema,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<RowData> partitionAssigner) {
        super(config);
        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");
        if (this.odpsConf.isClusterMode()) {
            throw new IllegalStateException("Odps sink function cannot support overwrite in cluster mode.");
        }
        this.projectName = Preconditions.checkNotNull(projectName, "project cannot be null");
        this.tableName = Preconditions.checkNotNull(tableName, "table cannot be null");
        this.dataSchema = schema;
        this.isDynamicPartition = isDynamicPartition;
        this.isPartitioned = !dataSchema.getPartitionKeys().isEmpty();
        this.supportsGrouping = supportsGrouping;
        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;
        if (isDynamicPartition && partitionAssigner == null) {
            this.partitionAssigner = new TablePartitionAssigner(
                            PartitionComputer.instance(dataSchema, partition, "__ODPS_DEFAULT_PARTITION__"));
        } else {
            this.partitionAssigner = partitionAssigner;
        }
        try {
            if (!isDynamicPartition) {
                checkPartition(partition);
            }
        } catch (IOException e) {
            throw new FlinkOdpsException(e);
        }
        LOG.info("Create odps upsert, table:{}.{}, partition:{},isDynamicPartition:{},supportsGrouping:{},writeOptions:{}",
                projectName, tableName, partition, isDynamicPartition, supportsGrouping, writeOptions);
    }

    private void checkPartition(String partitionSpec) throws IOException {
        if (isPartitioned) {
            if (StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                LOG.error("The partition cannot be null or whitespace with partition table: " + tableName);
                throw new IOException("Check partition failed.");
            } else {
                this.partition = new PartitionSpec(partitionSpec).toString();
            }
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                throw new IOException(
                        "The partition spec should be null or whitespace with non partition odps table: " + tableName);
            } else {
                this.partition = "";
            }
        }
    }

    private Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    @Override
    public void open(Configuration parameters) throws IOException {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint());
        RowType rowType =
                ((RowType) OdpsTableUtil.toRowDataType(dataSchema.getColumns()).getLogicalType());
        this.converter = RowDataToOdpsConverters.createRecordConverter(rowType);
        this.buckets = new HashMap<>();
        this.odpsUpsertWriterMap = new HashMap<>();
        this.odpsUpsertSessionMap = new HashMap<>();
        this.statuses = new ArrayList<>();
        this.commitConfirming = false;
    }

    @Override
    protected void snapshotState(long checkpoint) throws IOException {
        flushRemaining(false);
        while (commitConfirming) {
            try {
                this.executor.yield();
            } catch (InterruptedException e) {
                LOG.error("Snapshot state error: ", e);
            }
        }
    }

    @Override
    public void endInput() {
        super.endInput();
        try {
            flushRemaining(true);
        } catch (IOException e) {
            throw new FlinkOdpsException(e);
        }
    }

    private void flushRemaining(boolean endInput)
            throws IOException {
        boolean hasBufferedData = hasBufferedData();
        if (hasBufferedData) {
            while (hasPendingRequest()) {
                try {
                    this.executor.yield();
                } catch (InterruptedException e) {
                    LOG.error("End input error: ", e);
                }
            }
        }

        if (hasBufferedData) {
            buckets.forEach((partition, bucket) -> {
                if (!bucket.isEmpty()) {
                    try {
                        if (!odpsUpsertWriterMap.containsKey(partition)) {
                            initUpsertWriter(partition, sessionRequest.get(partition));
                        }
                        UpsertWriter<ArrayRecord> upsertWriter = odpsUpsertWriterMap.get(partition);
                        ArrayRecord record = upsertWriter.newElement();
                        List<DataItem> items = bucket.getRecords();
                        for (DataItem dataItem : items) {
                            ArrayRecord data = dataItem.getData();
                            for (int i = 0; i < record.getColumnCount(); i++) {
                                record.set(i, data.get(i));
                            }
                            doWriteRecord(partition, record, dataItem.getOperation());
                        }
                    } catch (IOException e) {
                        throw new FlinkOdpsException("Flush data error: ", e);
                    }
                    bucket.reset();
                }
            });
        }

        odpsUpsertWriterMap.forEach((part, upsertWriter) -> {
            TableUpsertSessionImpl session = odpsUpsertSessionMap.get(part);
            String sessionId = session.getId();
            try {
                upsertWriter.close();
                session.close();
            } catch (IOException e) {
                LOG.error("Close upsert writer error: ", e);
            }
            WriterStatus writerStatus = new WriterStatus();
            writerStatus.setSessionId(sessionId);
            writerStatus.setPartitionSpec(part);
            writerStatus.setTotalRecords(
                    upsertWriter.currentMetricsValues()
                            .counter("recordCount").get().getCount());
            statuses.add(writerStatus);
        });

        sendCommitEvent(endInput);

        buckets.clear();
        statuses.clear();
        sessionRequest.clear();
        odpsUpsertWriterMap.clear();
        odpsUpsertSessionMap.clear();
        commitConfirming = true;

        if (!isDynamicPartition) {
            // For bootstrap static partition
            sessionRequest.put(partition, "");
        }
    }

    @Override
    protected void sendBootstrapEvent() {
        if (!isDynamicPartition) {
            sendBootstrapEvent(partition);
        }
    }

    protected void sendBootstrapEvent(String partition) {
        LOG.info("Send bootstrap event to coordinator, task[{}], partition: {}.", taskID, partition);
        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .bootstrap(true)
                .requiredPartition(partition)
                .build();
        eventGateway.sendEventToCoordinator(event);
        sessionRequest.put(partition, "");
    }

    protected void sendCommitEvent(boolean endInput) {
        LOG.info("Send commit event to coordinator, task[{}].", taskID);
        SinkTaskEvent event = SinkTaskEvent.builder()
                .taskID(taskID)
                .bootstrap(false)
                .endInput(endInput)
                .writeStatus(statuses)
                .build();
        eventGateway.sendEventToCoordinator(event);
    }

    @Override
    public void processElement(RowData in, ProcessFunction<RowData, Object>.Context ctx,
                               Collector<Object> out) throws Exception {
        // TODO: For dynamic partition
        final RowKind kind = in.getRowKind();
        RecordOperationType type;
        if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
            type = RecordOperationType.UPSERT;
        } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))) {
            type = RecordOperationType.DELETE;
        } else {
            LOG.debug("Ignore row data {}.", in);
            return;
        }

        String currentPartition = "";
        if (!isDynamicPartition) {
            currentPartition = partition;
        } else {
            // TODO: support context
            currentPartition = partitionAssigner.getPartitionSpec(in, null);
        }

        if (!sessionRequest.containsKey(currentPartition)) {
            sendBootstrapEvent(currentPartition);
        }

        if (sessionRequest.get(currentPartition).isEmpty()) {
            // For no response
            final String targetPartitionSpec = currentPartition;
            DataBucket bucket = this.buckets.computeIfAbsent(targetPartitionSpec,
                    k -> new DataBucket(this.config.getLong(OdpsOptions.WRITE_BATCH_SIZE), targetPartitionSpec));
            final ArrayRecord record = new ArrayRecord(dataSchema.getColumns().toArray(new Column[0]));
            converter.convert(in, record);
            final DataItem item = new DataItem(currentPartition, record, type);
            bucket.add(item);
            if (bucket.getRecords().size() > 10000) {
                LOG.info("Slowly data {}.", in);
                Thread.sleep(1000);
            }
        } else {
            if (!odpsUpsertWriterMap.containsKey(currentPartition)) {
                initUpsertWriter(currentPartition, sessionRequest.get(currentPartition));
            }
            UpsertWriter<ArrayRecord> upsertWriter = odpsUpsertWriterMap.get(currentPartition);
            ArrayRecord record = upsertWriter.newElement();
            if (buckets.containsKey(currentPartition)) {
                DataBucket bucket = buckets.get(currentPartition);
                if (!bucket.isEmpty()) {
                    List<DataItem> items = bucket.getRecords();
                    for (DataItem dataItem : items) {
                        ArrayRecord data = dataItem.getData();
                        for (int i = 0; i < record.getColumnCount(); i++) {
                            record.set(i, data.get(i));
                        }
                        doWriteRecord(currentPartition, record, dataItem.getOperation());
                    }
                }
                bucket.reset();
            }
            converter.convert(in, record);
            doWriteRecord(currentPartition, record, type);
        }
    }

    @Override
    public void close() {
        odpsUpsertWriterMap.forEach((part, writer) -> {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Close upsert writer failed: ", e);
            }
        });
        odpsUpsertSessionMap.forEach((part, session) -> {
            try {
                session.close();
            } catch (IOException e) {
                LOG.error("Close upsert session failed: ", e);
            }
        });
    }

    @Override
    public void handleOperatorEvent(OperatorEvent event) {
        Preconditions.checkArgument(event instanceof TaskAckEvent,
                "The write function can only handle CommitAckEvent");
        TaskAckEvent ackEvent = (TaskAckEvent) event;
        LOG.info("Handle operator event for task[{}], {}", taskID, ackEvent);
        if (ackEvent.isCommitted()) {
            commitConfirming = false;
        }
        if (!ackEvent.getSessionId().isEmpty()) {
            sessionRequest.put(ackEvent.getPartition(),
                    ackEvent.getSessionId());
        }
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor executor) {
        this.executor = executor;
    }

    private void doWriteRecord(String partitionSpec, ArrayRecord record, RecordOperationType type)
            throws IOException {
        switch (type) {
            case UPSERT:
                odpsUpsertWriterMap.get(partitionSpec).upsert(record);
                break;
            case DELETE:
                odpsUpsertWriterMap.get(partitionSpec).delete(record);
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private UpsertWriter<ArrayRecord> initUpsertWriter(String targetPartition,
                                                       String sessionId) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder()
                .identifier(TableIdentifier.of(projectName, tableName))
                .withSessionProvider("upsert")
                .withSessionId(sessionId)
                .withSettings(settings);
        if (!StringUtils.isNullOrWhitespaceOnly(targetPartition)) {
            builder.partition(new PartitionSpec(targetPartition));
        }
        TableUpsertSessionImpl upsertSession =
                (TableUpsertSessionImpl) builder.buildUpsertSession();
        if (upsertSession.getStatus().equals(SessionStatus.NORMAL)) {
            odpsUpsertSessionMap.put(targetPartition, upsertSession);
            UpsertWriter<ArrayRecord> writer = upsertSession.createUpsertWriter();
            odpsUpsertWriterMap.put(targetPartition, writer);
            return writer;
        } else {
            throw new IOException("Invalid session status: " + upsertSession.getStatus());
        }
    }

    private boolean hasBufferedData() {
        return this.buckets.size() > 0
                && this.buckets.values().stream().anyMatch(bucket ->
                !bucket.isEmpty());
    }

    private boolean hasPendingRequest() {
        return this.sessionRequest.size() > 0
                && this.sessionRequest.values().stream().anyMatch(String::isEmpty);
    }
}