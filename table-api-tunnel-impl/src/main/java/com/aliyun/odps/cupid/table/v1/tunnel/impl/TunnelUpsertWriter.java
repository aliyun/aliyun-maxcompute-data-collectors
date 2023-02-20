package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TunnelUpsertWriter implements FileWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TunnelUpsertWriter.class);

    protected UpsertStream stream;
    protected TableTunnel.UpsertSession session;
    private boolean isClosed;
    private boolean isCommitted;
    private long rowsWritten;
    protected RetryStrategy commitRetry;

    TunnelUpsertWriter(TunnelWriteSessionInfo sessionInfo,
                       Map<String, String> partitionSpec) {
        PartitionSpec partition = Util.toOdpsPartitionSpec(partitionSpec);
        try {
            TableTunnel tunnel = Util.getTableTunnel(sessionInfo.getOptions());
            this.session = Util.getOrCreateUpsertSession(
                    sessionInfo.getProject(),
                    sessionInfo.getTable(),
                    partition,
                    tunnel,
                    sessionInfo.getUploadId());
            // TODO: listener
            UpsertStream.Listener listener = new UpsertStream.Listener() {
                @Override
                public void onFlush(UpsertStream.FlushResult result) {
                    LOG.info("flush success:" + result.traceId);
                }

                @Override
                public boolean onFlushFail(String error, int retry) {
                    LOG.info("flush failed:" + error);
                    return false;
                }
            };
            this.stream = session.buildUpsertStream().setListener(listener).build();
            this.isClosed = false;
            this.isCommitted = false;
            this.commitRetry = new RetryStrategy(3, 5, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
        } catch (IOException | TunnelException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ArrayRecord newElement() {
        return (ArrayRecord) this.session.newRecord();
    }

    @Override
    public void upsert(ArrayRecord data) throws IOException {
        try {
            this.stream.upsert(data);
            rowsWritten += 1;
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(ArrayRecord data) throws IOException {
        try {
            this.stream.delete(data);
            rowsWritten += 1;
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            this.stream.flush();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        // For 0.43.0
        // flush();
        // this.stream.close();
        try {
            this.stream.close();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
        isClosed = true;
    }

    @Override
    public void commit() throws IOException {
        if (isCommitted) {
            return;
        }
        close();
        tryCommit();
        isCommitted = true;
    }

    private void tryCommit() throws IOException {
        commitRetry.reset();
        while (true) {
            try {
                this.session.commit(false);
                break;
            } catch (Exception exception) {
                LOG.error(String.format("Commit error, retry times = %d",
                        commitRetry.getAttempts()), exception);
                try {
                    commitRetry.onFailure(exception);
                } catch (RetryExceedLimitException | InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    @Override
    public long getBytesWritten() {
        // TODO: Bytes
        return 0;
    }

    @Override
    public long getRowsWritten() {
        return rowsWritten;
    }

    @Override
    public void write(ArrayRecord data) throws IOException {
        throw new UnsupportedOperationException();
    }
}
