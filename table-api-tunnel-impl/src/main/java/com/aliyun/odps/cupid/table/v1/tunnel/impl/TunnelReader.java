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

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.SplitReader;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.aliyun.odps.type.TypeInfoParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TunnelReader implements SplitReader<ArrayRecord> {

    private final TunnelInputSplit inputSplit;
    private TunnelRecordReader reader;
    private Record currentRecord = null;
    private long rowsRead = 0;
    private boolean isClosed;

    TunnelReader(TunnelInputSplit inputSplit) {
        this.inputSplit = inputSplit;
        try {
            init();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void init() throws IOException {
        String project = inputSplit.getProject();
        String table = inputSplit.getTable();
        Map<String, String> partitionSpec = inputSplit.getPartitionSpec();
        String downloadId = inputSplit.getDownloadId();
        long startIndex = inputSplit.getStartIndex();
        long numRecord = inputSplit.getNumRecord();
        List<Attribute> requiredColumns = inputSplit.getReadDataColumns();

        TableTunnel tunnel = Util.getTableTunnel(inputSplit.getOptions());
        TableTunnel.DownloadSession session;
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            try {
                session = tunnel.getDownloadSession(project, table, downloadId);
            } catch (TunnelException e) {
                throw new IOException(e);
            }
        } else {
            try {
                PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partitionSpec);
                session = tunnel.getDownloadSession(project, table, odpsPartitionSpec, downloadId);
            } catch (TunnelException e) {
                throw new IOException(e);
            }
        }

        List<Column> readDataColumns = new ArrayList<>();
        for (Attribute c : requiredColumns) {
            readDataColumns.add(new Column(c.getName(), TypeInfoParser.getTypeInfoFromTypeString(c.getType())));
        }
        if (requiredColumns.isEmpty()) {
            List<Attribute> dataColumns = inputSplit.getDataColumns();
            if (!dataColumns.isEmpty()) {
                readDataColumns.add(new Column(dataColumns.get(0).getName(),TypeInfoParser.getTypeInfoFromTypeString(dataColumns.get(0).getType())));
            } else {
                throw new RuntimeException("Empty column is not supported by tunnel table provider");
            }
        }
        try {
            reader = session.openRecordReader(
                    startIndex, numRecord, true, readDataColumns);
        } catch (TunnelException e) {
            throw new IOException(e);
        }
        this.isClosed = false;
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        reader.close();
        isClosed = true;
    }

    @Override
    public long getBytesRead() {
        return reader.getTotalBytes();
    }

    @Override
    public long getRowsRead() {
        return rowsRead;
    }

    @Override
    public boolean hasNext() {
        return rowsRead < inputSplit.getNumRecord();
    }

    @Override
    public ArrayRecord next() {
        try {
            currentRecord = reader.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rowsRead += 1;
        return (ArrayRecord) currentRecord;
    }
}
