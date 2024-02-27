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
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.util.List;

public class TunnelRecordWriter implements BatchWriter<ArrayRecord> {

    protected com.aliyun.odps.tunnel.io.TunnelRecordWriter writer;
    private boolean isClosed;

    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    private final TableIdentifier identifier;
    private final List<Column> requiredDataColumns;
    private final String sinkId;
    private final long writerId;
    private final WriterOptions options;
    private final PartitionSpec partitionSpec;

    public TunnelRecordWriter(String sinkId,
                              TableIdentifier identifier,
                              DataSchema schema,
                              long blockNumber,
                              WriterOptions writerOptions,
                              PartitionSpec targetPartitionSpec)
            throws IOException {
        this.identifier = identifier;
        this.requiredDataColumns = schema.getColumns();
        this.sinkId = sinkId;
        this.writerId = blockNumber;
        this.options = writerOptions;
        this.partitionSpec = targetPartitionSpec;
        this.isClosed = false;
        initWriter();
        initMetrics();
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        this.metrics.register(bytesCount);
        this.metrics.register(recordCount);
    }

    @Override
    public ArrayRecord newElement() {
        return new ArrayRecord(this.requiredDataColumns.toArray(new Column[0]));
    }

    @Override
    public void write(ArrayRecord record) throws IOException {
        writer.write(record);
        recordCount.inc(1);
        bytesCount.setValue(writer.getTotalBytes());
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        close();
        return new TunnelCommitMessage(sinkId, writerId);
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        writer.close();
        isClosed = true;
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
            this.writer = (com.aliyun.odps.tunnel.io.TunnelRecordWriter)
                    uploadSession.openRecordWriter(writerId, true);
        } catch (TunnelException e) {
            throw new RuntimeException(e);
        }
    }
}
