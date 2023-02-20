package org.apache.flink.odps.output.writer.upsert;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.FileWriterBuilder;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.flink.odps.output.writer.OdpsTableWrite;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
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
import java.util.ArrayList;

import static com.aliyun.odps.cupid.table.v1.tunnel.impl.Util.UPSERT_ENABLE;

public class StaticOdpsPartitionUpsert extends OdpsTableWrite<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticOdpsPartitionUpsert.class);

    protected String currentPartition;
    protected DataStructureConverter<Object, Object> converter;
    protected RowUpsertWriter upsertWriter;

    public StaticOdpsPartitionUpsert(OdpsConf odpsConf,
                                     String projectName,
                                     String tableName,
                                     String partition,
                                     OdpsWriteOptions options) {
        super(odpsConf, projectName, tableName, partition, false, options);
        this.currentPartition = staticPartition;
        TableSchema flinkTableSchema =
                OdpsTableUtil.createTableSchema(getTableSchema().getColumns(), new ArrayList<>());
        this.converter = DataStructureConverters.getConverter(flinkTableSchema.toRowDataType());
    }

    @Override
    public void initWriteSession() throws IOException {
        Options options = OdpsUtils.getOdpsOptions(odpsConf);
        options.put(UPSERT_ENABLE, "true");
        createWriteSession(options, staticPartition);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            Preconditions.checkNotNull(writeSessionInfo, "Write session cannot be null!");
            if (useBatch) {
                throw new UnsupportedOperationException();
            } else {
                FileWriter<ArrayRecord> writer;
                if (isPartitioned) {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .partitionSpec(OdpsUtils.getPartitionSpecKVMap(new PartitionSpec(currentPartition)))
                            .buildRecordWriter();
                } else {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .buildRecordWriter();
                }
                upsertWriter = new RowUpsertWriter(
                        getTableSchema().getColumns().toArray(new Column[0]),
                        writer,
                        writeOptions) {
                };
            }
        } catch (ClassNotFoundException e) {
            LOG.error("Fail to init odps file writer!", e);
            throw new IOException("Fail to init odps file writer!", e);
        }
    }

    @Override
    public void writeRecord(RowData rowData) throws IOException {
        Object row = this.converter.toExternalOrNull(rowData);
        final RowKind kind = rowData.getRowKind();
        if (kind.equals(RowKind.INSERT) || kind.equals(RowKind.UPDATE_AFTER)) {
            this.upsertWriter.upsert((Row) row);
        } else if ((kind.equals(RowKind.DELETE) || kind.equals(RowKind.UPDATE_BEFORE))) {
            this.upsertWriter.delete((Row) row);
        } else {
            LOG.debug("Ignore row data {}.", rowData);
        }
    }

    @Override
    public void close() throws IOException {
        if (upsertWriter != null) {
            try {
                upsertWriter.close();
                upsertWriter.commit();
            } catch (Throwable e) {
                LOG.error("Failed to close odps writer!", e);
                throw new IOException(e);
            }
            upsertWriter = null;
            LOG.info("Finish to close odps writer");
        }
    }

    @Override
    public void commitWriteSession() throws IOException {
        if (tableWriteSession != null) {
            try {
                tableWriteSession.commitTable();
                tableWriteSession.cleanup();
            } catch (Throwable e) {
                LOG.error("Failed to commit odps write session!", e);
                throw new IOException(e);
            }
            tableWriteSession = null;
        }
    }
}
