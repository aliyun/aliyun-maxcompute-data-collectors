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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.table.tunnel.write;

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrowRecordWriter;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.odps.table.utils.ArrowSchemaUtils;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.util.List;

public class TunnelArrowBatchWriter implements BatchWriter<VectorSchemaRoot> {

    private boolean isClosed;
    private ArrowRecordWriter recordWriter;
    private final TableIdentifier identifier;
    private final List<Column> requiredDataColumns;
    private final String sinkId;
    private final long writerId;
    private final WriterOptions options;
    private final PartitionSpec partitionSpec;
    private final ArrowOptions arrowOptions;

    private WriterCommitMessage commitMessage;

    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    public TunnelArrowBatchWriter(String sinkId,
                                  TableIdentifier identifier,
                                  DataSchema schema,
                                  long blockNumber,
                                  WriterOptions writerOptions,
                                  PartitionSpec targetPartitionSpec,
                                  ArrowOptions arrowOptions) throws IOException {
        this.identifier = identifier;
        this.requiredDataColumns = schema.getColumns();
        this.sinkId = sinkId;
        this.writerId = blockNumber;
        this.options = writerOptions;
        this.partitionSpec = targetPartitionSpec;
        this.arrowOptions = validateArrowOptions(arrowOptions);
        this.isClosed = false;
        initMetrics();
    }

    private ArrowOptions validateArrowOptions(ArrowOptions options) {
        // TODO: now hard code arrow timestamp nanos
        // Preconditions.checkArgument(options.getTimestampUnit().equals(ArrowOptions.DEFAULT_TIMESTAMP_UNIT),
        // "Unsupported timestamp unit: " + options.getTimestampUnit());
        return ArrowOptions.newBuilder()
                .withTimestampUnit(ArrowOptions.DEFAULT_TIMESTAMP_UNIT)
                .build();
    }

    @Override
    public VectorSchemaRoot newElement() {
        // TODO: use odps sdk schema utils
        Schema arrowSchema = ArrowSchemaUtils.toArrowSchema(requiredDataColumns, arrowOptions);
        return VectorSchemaRoot.create(arrowSchema, options.getBufferAllocator());
    }

    // TODO: lazy create writer
    @Override
    public void write(VectorSchemaRoot root) throws IOException {
        if (isClosed) {
            throw new IOException("Arrow writer is closed");
        }
        if (this.recordWriter == null) {
            initWriter();
        }
        this.recordWriter.write(root);
        recordCount.inc(root.getRowCount());
        bytesCount.setValue(recordWriter.bytesWritten());
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        close();
        return commitMessage;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            try {
                if (recordWriter != null) {
                    recordWriter.close();
                    commitMessage = new TunnelCommitMessage(sinkId, writerId);
                }
            } finally {
                isClosed = true;
            }
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

    private void initWriter() throws IOException {
        try {
            TableTunnel tunnel = TableUtils.getTableTunnel(options.getSettings());
            TableTunnel.UploadSession uploadSession;
            // TODO: support schema
            if (partitionSpec == null || partitionSpec.keys().size() == 0) {
                uploadSession = tunnel.getUploadSession(
                        identifier.getProject(),
                        identifier.getTable(),
                        sinkId);
            } else {
                uploadSession = tunnel.getUploadSession(
                        identifier.getProject(),
                        identifier.getTable(),
                        partitionSpec,
                        sinkId);
            }
            // TODO: compression
            this.recordWriter = uploadSession.openArrowRecordWriter(writerId);
        } catch (TunnelException e) {
            throw new RuntimeException(e);
        }
    }
}