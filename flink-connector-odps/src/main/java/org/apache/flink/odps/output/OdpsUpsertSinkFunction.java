package org.apache.flink.odps.output;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.*;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class OdpsUpsertSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsUpsertSinkFunction.class);

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private final String partition;
    private final OdpsWriteOptions writeOptions;
    private final PartitionAssigner<Row> partitionAssigner;
    private final boolean isDynamicPartition;
    private final boolean supportsGrouping;

    private transient OdpsUpsert<Row> odpsWrite;
    private transient volatile Exception flushException;
    private transient volatile boolean closed;
    private transient int taskNumber;
    private transient int numTasks;
    private transient DataStructureConverter<Object, Object> converter;

    public OdpsUpsertSinkFunction(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<Row> partitionAssigner) {
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
        this.partition = partition;
        this.isDynamicPartition = isDynamicPartition;
        this.supportsGrouping = supportsGrouping;
        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;
        this.partitionAssigner = partitionAssigner;
        LOG.info("Create odps upsert, table:{}.{}, partition:{},isDynamicPartition:{},supportsGrouping:{},writeOptions:{}",
                projectName, tableName, partition, isDynamicPartition, supportsGrouping, writeOptions);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        synchronized (this) {
            if (!closed) {
                try {
                    if (odpsWrite != null) {
                        odpsWrite.close();
                        // TODO: upsert session
                        // odpsWrite.commitWriteSession();
                        // odpsWrite.initWriteSession();
                        odpsWrite.open(taskNumber, numTasks);
                    }
                } catch (Exception e) {
                    flushException = e;
                }
            }
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        this.odpsWrite = OdpsWriteFactory.createOdpsUpsert(
                odpsConf,
                projectName,
                tableName,
                partition,
                isDynamicPartition,
                supportsGrouping,
                writeOptions,
                partitionAssigner);
        RuntimeContext ctx = getRuntimeContext();
        this.closed = false;
        this.taskNumber = ctx.getIndexOfThisSubtask();
        this.numTasks = ctx.getNumberOfParallelSubtasks();
        com.aliyun.odps.TableSchema odpsTableSchema = ((OdpsTableWrite)odpsWrite).getTableSchema();
        TableSchema flinkTableSchema =
                OdpsTableUtil.createTableSchema(odpsTableSchema.getColumns(), odpsTableSchema.getPartitionColumns());
        this.converter = DataStructureConverters.getConverter(flinkTableSchema.toRowDataType());
        odpsWrite.initWriteSession();
        odpsWrite.open(taskNumber, numTasks);
        LOG.info("Open odps upsert function");
    }

    @Override
    public final void invoke(RowData value, Context context) throws Exception {
        synchronized (this) {
            checkFlushException();
            try {
                odpsWrite.updateWriteContext(context);
                Object row = this.converter.toExternalOrNull(value);
                final RowKind kind = value.getRowKind();
                if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
                    odpsWrite.upsert((Row) row);
                } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))) {
                    odpsWrite.delete((Row) row);
                } else {
                    LOG.debug("Ignore row data {}.", value);
                }
            } catch (Exception e) {
                throw new IOException("Writing records to Odps failed.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        synchronized (this) {
            if (!closed) {
                closed = true;
                if (odpsWrite != null) {
                    try {
                        odpsWrite.close();
                        // TODO: upsert session
                        // odpsWrite.commitWriteSession();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
            checkFlushException();
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Odps failed.", flushException);
        }
    }

    /**
     * Builder to build {@link OdpsSinkFunction}.
     */
    public static class OdpsUpsertSinkBuilder {
        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String partition;
        private OdpsWriteOptions writeOptions;
        private boolean isDynamicPartition;
        private boolean supportPartitionGrouping;
        private PartitionAssigner<Row> partitionAssigner;

        public OdpsUpsertSinkBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OdpsUpsertSinkBuilder(OdpsConf odpsConf,
                                 String projectName,
                                 String tableName) {
            this(odpsConf, projectName, tableName, null);
        }

        public OdpsUpsertSinkBuilder(OdpsConf odpsConf,
                                 String projectName,
                                 String tableName,
                                 OdpsWriteOptions writeOptions) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
            this.writeOptions = writeOptions;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setOdpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setWriteOptions(OdpsWriteOptions writeOptions) {
            this.writeOptions = writeOptions;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setDynamicPartition(boolean dynamicPartition) {
            this.isDynamicPartition = dynamicPartition;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setSupportPartitionGrouping(boolean supportPartitionGrouping) {
            this.supportPartitionGrouping = supportPartitionGrouping;
            return this;
        }

        public OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder setPartitionAssigner(PartitionAssigner<Row> partitionAssigner) {
            this.partitionAssigner = partitionAssigner;
            return this;
        }

        public OdpsUpsertSinkFunction build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");
            return new OdpsUpsertSinkFunction(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    isDynamicPartition,
                    supportPartitionGrouping,
                    writeOptions,
                    partitionAssigner);
        }
    }
}
