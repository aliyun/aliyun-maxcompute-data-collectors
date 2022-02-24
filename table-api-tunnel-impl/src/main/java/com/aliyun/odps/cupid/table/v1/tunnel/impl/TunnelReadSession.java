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
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.reader.RequiredSchema;
import com.aliyun.odps.cupid.table.v1.reader.TableReadSession;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.Validator;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TunnelReadSession extends TableReadSession {

    private final static long DEFAULT_AVERAGE_RECORD_SIZE = 1024;
    private final static long MIN_AVERAGE_RECORD_SIZE = 256;

    private InputSplit[] inputSplits = null;
    private List<Attribute> dataColumns;
    private List<Attribute> partitionColumns;
    private List<Attribute> requiredColumns;
    private Odps odps;

    TunnelReadSession(String project,
                      String table,
                      TableSchema tableSchema,
                      RequiredSchema readDataColumns,
                      List<Map<String, String>> partitionSpecs,
                      Options options) {
        super(project, table, options, tableSchema, readDataColumns, partitionSpecs);
        initOdps();
    }

    public TableSchema getTableSchema() {
        if (this.tableSchema == null) {
            if (this.odps == null) {
                initOdps();
            }
            this.tableSchema = odps.tables().get(project, table).getSchema();
        }
        return this.tableSchema;
    }

    @Override
    public InputSplit[] getOrCreateInputSplits() throws IOException {
        if (this.splitParallelism > 0) {
            throw new UnsupportedOperationException(
                    "split by parallelism is not supported by tunnel provider");
        } else {
            return getOrCreateInputSplits(this.splitSizeInMB);
        }
    }

    /**
     * Since split table by record count is more convenient when reading a table with tunnel, here
     * we estimate the average size of a record and the record count per split would be
     * splitSizeInMB divided by the average size. Then we split the table by the record count.
     *
     * @param splitSizeInMB size of each split
     * @return an array of {@link InputSplit}
     * @throws IOException IOException is thrown when create tunnel download session failed
     */
    @Override
    public InputSplit[] getOrCreateInputSplits(int splitSizeInMB) throws IOException {
        if (splitSizeInMB <= 0) {
            throw new IllegalArgumentException("Expect positive split size, got: " + splitSizeInMB);
        }

        if (inputSplits != null) {
            return inputSplits;
        }

        List<InputSplit> splits = new ArrayList<>();
        TableTunnel tunnel = new TableTunnel(odps);
        if (!StringUtils.isNullOrEmpty(this.options.getOdpsConf().getTunnelEndpoint())) {
            tunnel.setEndpoint(this.options.getOdpsConf().getTunnelEndpoint());
        }

        init();

        if (partitionSpecs == null || partitionSpecs.isEmpty()) {
            long size = odps.tables().get(project, table).getSize();
            TableTunnel.DownloadSession session = Util.createDownloadSession(project, table, null, tunnel);
            splits.addAll(
                    getInputSplitsInternal(session, size, splitSizeInMB, null));
        } else {
            for (Map<String, String> partitionSpec : partitionSpecs) {
                PartitionSpec odpsPartitionSpec = Util.toOdpsPartitionSpec(partitionSpec);
                long size = odps.tables().get(project, table).getPartition(odpsPartitionSpec).getSize();
                TableTunnel.DownloadSession session = Util.createDownloadSession(
                        project,
                        table,
                        odpsPartitionSpec,
                        tunnel);
                splits.addAll(
                        getInputSplitsInternal(session, size, splitSizeInMB, partitionSpec));
            }
        }

        // Cache the result
        inputSplits = splits.toArray(new InputSplit[0]);

        return inputSplits;
    }

    /**
     * Generate serializable data columns, partition columns and required columns.
     */
    private void init() {
        if (tableSchema == null) {
            tableSchema = getTableSchema();
        }

        dataColumns = new ArrayList<>();
        for (Column c : tableSchema.getColumns()) {
            String type = c.getTypeInfo().getTypeName();
            dataColumns.add(new Attribute(c.getName(), type));
        }
        partitionColumns = new ArrayList<>();
        for (Column c : tableSchema.getPartitionColumns()) {
            String type = c.getTypeInfo().getTypeName();
            partitionColumns.add(new Attribute(c.getName(), type));
        }
        requiredColumns = new ArrayList<>();
        if (readDataColumns.getType().equals(RequiredSchema.Type.COLUMNS_SPECIFIED)) {
            Set<String> readColumnsName = readDataColumns.toList()
                    .stream()
                    .map(Attribute::getName)
                    .collect(Collectors.toSet());
            requiredColumns = this.dataColumns
                    .stream()
                    .filter(attr -> readColumnsName.contains(attr.getName()))
                    .collect(Collectors.toList());
        } else {
            requiredColumns.addAll(dataColumns);
        }
    }

    private List<InputSplit> getInputSplitsInternal(TableTunnel.DownloadSession session,
                                                    long size,
                                                    int splitSizeInMB,
                                                    Map<String, String> partitionSpec) {
        List<InputSplit> splits = new ArrayList<>();
        String downloadId = session.getId();
        long recordCount = session.getRecordCount();

        long averageRecordSize;
        if (recordCount == 0) {
            averageRecordSize = DEFAULT_AVERAGE_RECORD_SIZE;
        } else {
            averageRecordSize = size / recordCount;
        }
        if (averageRecordSize == 0) {
            averageRecordSize = MIN_AVERAGE_RECORD_SIZE;
        }
        long numRecordPerSplit = splitSizeInMB * 1024 * 1024 / averageRecordSize;
        if (numRecordPerSplit == 0) {
            throw new IllegalArgumentException("Expect larger split size, got: " + splitSizeInMB);
        }

        long numSplits = recordCount / numRecordPerSplit;
        long remainder = recordCount % numRecordPerSplit;
        for (long i = 0; i < numSplits; i++) {
            long startIndex = i * numRecordPerSplit;
            TunnelInputSplit split = new TunnelInputSplit(project, table, dataColumns,
                    partitionColumns, requiredColumns, partitionSpec, downloadId, startIndex,
                    numRecordPerSplit, options);
            splits.add(split);
        }

        if (remainder != 0) {
            long startIndex = numSplits * numRecordPerSplit;
            TunnelInputSplit lastSplit = new TunnelInputSplit(project, table, dataColumns,
                    partitionColumns, requiredColumns, partitionSpec, downloadId, startIndex,
                    remainder, options);
            splits.add(lastSplit);
        }

        return splits;
    }

    private void initOdps() {
        Validator.checkNotNull(this.options, "options");
        if (this.odps == null) {
            this.odps = Util.getOdps(this.options);
        }
    }
}
