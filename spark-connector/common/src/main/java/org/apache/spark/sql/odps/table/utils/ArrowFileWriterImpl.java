package org.apache.spark.sql.odps.table.utils;

import com.aliyun.odps.table.arrow.ArrowWriter;
import com.aliyun.odps.table.arrow.ArrowWriterFactory;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterCommitMessage;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.FileOutputStream;
import java.io.IOException;

public class ArrowFileWriterImpl implements BatchWriter<VectorSchemaRoot> {

    private boolean isClosed;
    private final WriterOptions writerOptions;
    private ArrowWriter batchWriter;
    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;
    private final FileOutputStream fos;

    public ArrowFileWriterImpl(WriterOptions writerOptions,
                           FileOutputStream fos) {
        this.writerOptions = writerOptions;
        this.isClosed = false;
        this.fos = fos;
        initMetrics();
    }

    @Override
    public VectorSchemaRoot newElement() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        if (isClosed) {
            throw new IOException("Arrow writer is closed");
        }

        if (batchWriter == null) {
            batchWriter = ArrowWriterFactory.getRecordBatchWriter(fos, writerOptions);
        }
        try {
            batchWriter.writeBatch(root);
            recordCount.inc(root.getRowCount());
            bytesCount.setValue(batchWriter.bytesWritten());
        } catch (IOException e) {
            throw new IOException("ArrowOutputStream Serialize Exception", e);
        }
    }

    @Override
    public void abort() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            if (batchWriter != null) {
                batchWriter.close();
            }
            isClosed = true;
        }
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        this.metrics.register(bytesCount);
        this.metrics.register(recordCount);
    }
}
