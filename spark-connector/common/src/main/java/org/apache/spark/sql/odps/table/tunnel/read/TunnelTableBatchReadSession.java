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

package org.apache.spark.sql.odps.table.tunnel.read;

import com.aliyun.odps.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.read.impl.batch.TableBatchReadSessionBase;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TunnelTableBatchReadSession extends TableBatchReadSessionBase {

    private final static long DEFAULT_AVERAGE_RECORD_SIZE = 1024;
    private final static long MIN_AVERAGE_RECORD_SIZE = 256;

    public TunnelTableBatchReadSession(TableIdentifier identifier,
                                       List<PartitionSpec> includedPartitions,
                                       List<String> requiredDataColumns,
                                       List<String> requiredPartitionColumns,
                                       List<Integer> bucketIds,
                                       SplitOptions splitOptions,
                                       ArrowOptions arrowOptions,
                                       EnvironmentSettings settings) throws IOException {
        super(identifier, includedPartitions, requiredDataColumns,
                requiredPartitionColumns, bucketIds, splitOptions, arrowOptions, settings);
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (dataFormat.getType().equals(DataFormat.Type.RECORD)) {
            return true;
        }
        if (dataFormat.getType().equals(DataFormat.Type.ARROW)) {
            return readSchema.getColumns().stream()
                    .map(Column::getType)
                    .noneMatch(odpsType -> odpsType.equals(OdpsType.STRUCT)
                            || odpsType.equals(OdpsType.ARRAY)
                            || odpsType.equals(OdpsType.MAP));
        }
        return false;
    }

    @Override
    public SplitReader<VectorSchemaRoot> createArrowReader(InputSplit split, ReaderOptions options) throws IOException {
        Preconditions.checkArgument(split instanceof TunnelInputSplit,
                "Input split: " + split.getClass().getName());
        return new TunnelArrowSplitReader(identifier, (TunnelInputSplit) split, readSchema, options);
    }

    @Override
    public SplitReader<ArrayRecord> createRecordReader(InputSplit split, ReaderOptions options) throws IOException {
        Preconditions.checkArgument(split instanceof TunnelInputSplit,
                "Input split: " + split.getClass().getName());
        return new TunnelSplitRecordReader(identifier, (TunnelInputSplit) split, readSchema, options);
    }

    @Override
    protected void planInputSplits() throws IOException {
        if (requiredBucketIds.size() > 0) {
            throw new UnsupportedOperationException("Unsupported bucket pruning in tunnel env");
        }

        if (splitOptions.getSplitMode().equals(SplitOptions.SplitMode.SIZE) ||
                splitOptions.getSplitMode().equals(SplitOptions.SplitMode.ROW_OFFSET)) {
            ExecutionEnvironment env = ExecutionEnvironment.create(settings);
            Odps odps = env.createOdpsClient();
            TableTunnel tunnel = new TableTunnel(odps);
            tunnel.setEndpoint(env.getTunnelEndpoint(identifier.getProject()));
            Table table = odps.tables().get(identifier.getProject(),
                    identifier.getSchema(),
                    identifier.getTable());
            TableSchema tableSchema = table.getSchema();
            long splitSizeInBytes = splitOptions.getSplitNumber();
            try {
                List<TunnelInputSplit> splits = new ArrayList<>();
                List<Column> requiredColumns = new ArrayList<>();
                List<String> partitionKeys = new ArrayList<>();

                if (table.isPartitioned()) {
                    // TODO: public odps sdk need table.getPartitionSpecs();
                    List<PartitionSpec> readPartitions = requiredPartitions.size() > 0 ?
                            requiredPartitions :
                            table.getPartitions().stream().map(Partition::getPartitionSpec).collect(Collectors.toList());
                    for (PartitionSpec partitionSpec : readPartitions) {
                        long size = table.getPartition(partitionSpec).getSize();
                        TableTunnel.DownloadSession session = createDownloadSession(
                                identifier.getProject(),
                                identifier.getSchema(),
                                identifier.getTable(),
                                partitionSpec,
                                tunnel);
                        splits.addAll(
                                getInputSplitsInternal(session, size, splitSizeInBytes, partitionSpec, splitOptions));
                    }

                    if (requiredDataColumns.size() == 0 &&
                            requiredPartitionColumns.size() == 0) {
                        requiredColumns.addAll(tableSchema.getColumns());
                        requiredColumns.addAll(tableSchema.getPartitionColumns());
                        partitionKeys = tableSchema.getPartitionColumns().stream()
                                .map(Column::getName).collect(Collectors.toList());
                    } else {
                        if (requiredDataColumns.size() > 0) {
                            TableUtils.validateRequiredDataColumns(requiredDataColumns,
                                    tableSchema.getColumns());
                            requiredColumns.addAll(requiredDataColumns
                                    .stream()
                                    .map(name -> tableSchema.getColumn(name.toLowerCase()))
                                    .collect(Collectors.toList()));
                        }

                        if (requiredPartitionColumns.size() > 0) {
                            TableUtils.validateRequiredPartitionColumns(requiredPartitionColumns,
                                    tableSchema.getPartitionColumns());
                            requiredColumns.addAll(requiredPartitionColumns
                                    .stream()
                                    .map(name -> tableSchema.getPartitionColumn(name.toLowerCase()))
                                    .collect(Collectors.toList()));
                            partitionKeys = requiredPartitionColumns;
                        }
                    }
                } else {
                    if (requiredPartitions.size() > 0) {
                        throw new UnsupportedOperationException(
                                "Partition filter not supported for none partitioned table");
                    }

                    long size = table.getSize();
                    TableTunnel.DownloadSession session = createDownloadSession(
                            identifier.getProject(),
                            identifier.getSchema(),
                            identifier.getTable(),
                            null,
                            tunnel);
                    splits.addAll(
                            getInputSplitsInternal(session, size, splitSizeInBytes, null, splitOptions));

                    TableUtils.validateRequiredDataColumns(requiredDataColumns,
                            tableSchema.getColumns());

                    requiredColumns = requiredDataColumns.size() > 0 ?
                            requiredDataColumns.stream()
                                    .map(name -> tableSchema.getColumn(name.toLowerCase()))
                                    .collect(Collectors.toList()) : tableSchema.getColumns();
                }

                this.readSchema = DataSchema.newBuilder()
                        .columns(requiredColumns)
                        .partitionBy(partitionKeys)
                        .build();

                if (splitOptions.getSplitMode().equals(SplitOptions.SplitMode.SIZE)) {
                    this.inputSplitAssigner = new TunnelInputSplitAssigner(splits);
                } else {
                    this.inputSplitAssigner = new TunnelRowRangeInputSplitAssigner(splits);
                }
                // TODO: this.id = ?
            } catch (OdpsException exception) {
                throw new IOException(exception);
            }
        } else {
            throw new UnsupportedOperationException(
                    "Split mode: '" + splitOptions.getSplitMode() + "' not supported");
        }
    }

    @Override
    protected String reloadInputSplits() throws IOException {
        throw new UnsupportedOperationException("Unsupported reload tunnel read session!");
    }

    private List<TunnelInputSplit> getInputSplitsInternal(TableTunnel.DownloadSession session,
                                                          long size,
                                                          long splitSizeInBytes,
                                                          PartitionSpec partitionSpec,
                                                          SplitOptions splitOptions) {
        List<TunnelInputSplit> splits = new ArrayList<>();
        String downloadId = session.getId();
        long recordCount = session.getRecordCount();

        if (splitOptions.getSplitMode().equals(SplitOptions.SplitMode.ROW_OFFSET)) {
            TunnelInputSplit lastSplit = new TunnelInputSplit(
                    downloadId, 0, recordCount, partitionSpec);
            splits.add(lastSplit);
        } else {
            long averageRecordSize;
            if (recordCount == 0) {
                averageRecordSize = DEFAULT_AVERAGE_RECORD_SIZE;
            } else {
                averageRecordSize = size / recordCount;
            }
            if (averageRecordSize == 0) {
                averageRecordSize = MIN_AVERAGE_RECORD_SIZE;
            }
            long numRecordPerSplit = splitSizeInBytes / averageRecordSize;
            if (numRecordPerSplit == 0) {
                throw new IllegalArgumentException("Expect larger split size, got: " + splitSizeInBytes);
            }

            long numSplits = recordCount / numRecordPerSplit;
            long remainder = recordCount % numRecordPerSplit;
            for (long i = 0; i < numSplits; i++) {
                long startIndex = i * numRecordPerSplit;
                TunnelInputSplit split = new TunnelInputSplit(downloadId, startIndex,
                        numRecordPerSplit, partitionSpec);
                splits.add(split);
            }

            if (remainder != 0) {
                long startIndex = numSplits * numRecordPerSplit;
                TunnelInputSplit lastSplit = new TunnelInputSplit(downloadId, startIndex,
                        remainder, partitionSpec);
                splits.add(lastSplit);
            }
        }
        return splits;
    }

    public static TableTunnel.DownloadSession createDownloadSession(String project,
                                                                    String schema,
                                                                    String table,
                                                                    PartitionSpec partitionSpec,
                                                                    TableTunnel tunnel) throws IOException {
        int retry = 0;
        long sleep = 2000;
        TableTunnel.DownloadSession downloadSession;
        while (true) {
            try {
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    downloadSession = tunnel.createDownloadSession(project, schema,
                            table, false);
                } else {
                    downloadSession = tunnel.createDownloadSession(project, schema,
                            table,
                            partitionSpec, false);
                }
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
        return downloadSession;
    }
}
