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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.tunnel.io.TunnelRecordWriter;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Map;

public class TunnelWriter implements FileWriter<ArrayRecord> {

    protected TunnelWriteSessionInfo sessionInfo;
    protected long blockId;
    protected TableTunnel.UploadSession session;
    protected RecordWriter writer;
    private long rowsWritten;
    private boolean isClosed;
    private String uploadId;
    private boolean isBufferWriter;
    private final Map<String, String> partitionSpec;
    private final Odps odps;

    TunnelWriter(TunnelWriteSessionInfo sessionInfo, long blockId, Map<String, String> partitionSpec) {
        this.sessionInfo = sessionInfo;
        this.blockId = blockId;
        this.isClosed = false;
        this.partitionSpec = partitionSpec;
        this.odps = Util.getOdps(sessionInfo.getOptions());
        try {
            init();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() throws IOException {
        try {
            uploadId = sessionInfo.getUploadId();
            isBufferWriter = sessionInfo.getOptions().
                    getOrDefault(Util.WRITER_BUFFER_ENABLE, false);
            if (sessionInfo.isDynamicPartition()) {
                initDynamicWriter();
            } else {
                initStaticWriter();
            }
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(ArrayRecord data) throws IOException {
        writer.write(data);
        rowsWritten += 1;
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        writer.close();
        isClosed = true;
    }

    @Override
    public void commit() throws IOException {
        close();
    }

    @Override
    public WriterCommitMessage commitWithResult() throws IOException {
        close();
        if (sessionInfo.isDynamicPartition()) {
            return new TunnelDynamicWriteMsg(sessionInfo.getProject(),
                    sessionInfo.getTable(),
                    partitionSpec,
                    uploadId);
        } else {
            return new TunnelWriteMsg();
        }
    }

    @Override
    public long getBytesWritten() {
        if (isBufferWriter) {
            try {
                return ((TunnelBufferedWriter) writer).getTotalBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return ((TunnelRecordWriter) writer).getTotalBytes();
        }
    }

    @Override
    public long getRowsWritten() {
        return rowsWritten;
    }

    private void initDynamicWriter() throws IOException, TunnelException {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            throw new InvalidParameterException("Tunnel dynamic partition is empty");
        } else {
            TableTunnel tunnel = Util.getTableTunnel(sessionInfo.getOptions());
            PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partitionSpec);
            Util.createPartition(sessionInfo.getProject(), sessionInfo.getTable(), odpsPartitionSpec, odps);
            session = Util.createUploadSession(sessionInfo.getProject(),
                    sessionInfo.getTable(),
                    odpsPartitionSpec,
                    sessionInfo.isOverwrite(),
                    tunnel);
            uploadId = session.getId();
        }
        if (isBufferWriter) {
            writer = session.openBufferedWriter(true);
            ((TunnelBufferedWriter)writer).setBufferSize(
                    sessionInfo.getOptions().getOrDefault(Util.WRITER_BUFFER_SIZE, Util.DEFAULT_WRITER_BUFFER_SIZE));
        } else {
            writer = session.openRecordWriter(0,true);
        }
    }

    private void initStaticWriter() throws IOException, TunnelException {
        String project = sessionInfo.getProject();
        String table = sessionInfo.getTable();
        Map<String, String> partitionSpec = sessionInfo.getPartitionSpec();
        TableTunnel tunnel = Util.getTableTunnel(sessionInfo.getOptions());
        if (isBufferWriter) {
            int shares = sessionInfo.getOptions().getOrDefault(Util.WRITER_BUFFER_SHARES, 1);
            if (partitionSpec == null || partitionSpec.isEmpty()) {
                this.session = tunnel.getUploadSession(project, table, uploadId, shares, blockId);
            } else {
                PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partitionSpec);
                this.session = tunnel.getUploadSession(project, table, odpsPartitionSpec, uploadId, shares, blockId);
            }
            writer = session.openBufferedWriter(true);
            ((TunnelBufferedWriter)writer).setBufferSize(
                    sessionInfo.getOptions().getOrDefault(Util.WRITER_BUFFER_SIZE, Util.DEFAULT_WRITER_BUFFER_SIZE));

        } else {
            if (partitionSpec == null || partitionSpec.isEmpty()) {
                session = tunnel.getUploadSession(project, table, uploadId);
            } else {
                PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partitionSpec);
                session = tunnel.getUploadSession(project, table, odpsPartitionSpec, uploadId);
            }
            writer = session.openRecordWriter(blockId,true);
        }
    }
}
