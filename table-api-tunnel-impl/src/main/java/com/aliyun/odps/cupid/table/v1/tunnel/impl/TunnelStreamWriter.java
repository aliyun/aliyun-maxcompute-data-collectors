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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class TunnelStreamWriter implements FileWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TunnelStreamWriter.class);

    protected TunnelWriteSessionInfo sessionInfo;
    protected TableTunnel.StreamUploadSession session;
    protected TableTunnel.StreamRecordPack pack;
    private long rowsWritten;
    private long bytesWritten;
    private PartitionSpec partition;

    TunnelStreamWriter(TunnelWriteSessionInfo sessionInfo,
                       Map<String, String> partitionSpec) {
        this.sessionInfo = sessionInfo;
        this.partition = Util.toOdpsPartitionSpec(partitionSpec);
        try {
            this.session = Util.createStreamUploadSession(
                    sessionInfo.getProject(),
                    sessionInfo.getTable(),
                    partition,
                    true,
                    Util.getTableTunnel(sessionInfo.getOptions()));
            this.pack = session.newRecordPack();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(ArrayRecord data) throws IOException {
        pack.append(data);
    }

    @Override
    public void flush() throws IOException {
        if (this.pack.getRecordCount() != 0L) {
            TableTunnel.FlushResult result = this.pack.flush(new TableTunnel.FlushOption());
            LOG.info("Trace ID:" + result.getTraceId() +
                    ", Size:" + result.getFlushSize() +
                    ", Record Count:" + result.getRecordCount() +
                    ", Partition:" + partition);
            this.rowsWritten += result.getRecordCount();
            this.bytesWritten += result.getFlushSize();
        }
    }

    @Override
    public long getBufferBytes() {
        return this.pack.getDataSize();
    }

    @Override
    public long getBufferRows() {
        return this.pack.getRecordCount();
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void commit() throws IOException {
        close();
    }

    @Override
    public WriterCommitMessage commitWithResult() throws IOException {
        return new TunnelWriteMsg();
    }

    @Override
    public long getBytesWritten() {
        return bytesWritten;
    }

    @Override
    public long getRowsWritten() {
        return rowsWritten;
    }
}
