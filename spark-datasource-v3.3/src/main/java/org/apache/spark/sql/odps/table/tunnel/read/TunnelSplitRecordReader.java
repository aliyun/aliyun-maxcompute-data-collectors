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

import com.aliyun.odps.Column;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TunnelSplitRecordReader implements SplitReader<ArrayRecord> {

    private TunnelRecordReader reader;
    private boolean isClosed;
    private final TunnelInputSplit inputSplit;
    private Record currentRecord = null;

    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    private List<Object> partitionValues;
    private int dataColumnsSize;

    public TunnelSplitRecordReader(TableIdentifier identifier,
                                   TunnelInputSplit split,
                                   DataSchema requiredSchema,
                                   ReaderOptions options) throws IOException {
        this.isClosed = false;
        this.inputSplit = split;
        init(identifier, split, requiredSchema, options);
        initMetrics();
    }

    @Override
    public boolean hasNext() throws IOException {
        return recordCount.getCount() < inputSplit.getRowRange().getNumRecord();
    }

    @Override
    public ArrayRecord get() {
        if (dataColumnsSize > 0) {
            try {
                currentRecord = reader.read(currentRecord);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        setPartitionValues();
        recordCount.inc(1);
        return (ArrayRecord) currentRecord;
    }

    @Override
    public Metrics currentMetricsValues() {
        if (dataColumnsSize > 0) {
            bytesCount.setValue(reader.getTotalBytes());
        }
        return metrics;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed) {
            if (reader != null) {
                reader.close();
            }
            isClosed = true;
        }
    }

    private void setPartitionValues() {
        for (int i = 0; i < partitionValues.size(); i++) {
            this.currentRecord.set(dataColumnsSize + i, partitionValues.get(i));
        }
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        this.metrics.register(bytesCount);
        this.metrics.register(recordCount);
    }

    private void init(TableIdentifier identifier,
                      TunnelInputSplit split,
                      DataSchema requiredSchema,
                      ReaderOptions options) throws IOException {
        String project = identifier.getProject();
        String table = identifier.getTable();
        // TODO: support schema
        PartitionSpec partitionSpec = split.getPartitionSpec();
        String downloadId = split.getSessionId();
        long startIndex = split.getRowRange().getStartIndex();
        long numRecord = split.getRowRange().getNumRecord();

        Set<String> partitionKeys = new HashSet<>(requiredSchema.getPartitionKeys());
        List<Column> readDataColumns = requiredSchema.getColumns()
                .stream()
                .filter(c -> !partitionKeys.contains(c.getName()))
                .collect(Collectors.toList());
        this.dataColumnsSize = readDataColumns.size();
        if (dataColumnsSize > 0) {
            // if data columns size > 0, then create reader
            try {
                TableTunnel tunnel = TableUtils.getTableTunnel(options.getSettings());
                TableTunnel.DownloadSession session;
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    session = tunnel.getDownloadSession(project, table, downloadId);
                } else {
                    session = tunnel.getDownloadSession(project, table, partitionSpec, downloadId);
                }
                this.reader = session.openRecordReader(
                        startIndex, numRecord, true, readDataColumns);
            } catch (TunnelException e) {
                throw new IOException(e);
            }
        }

        this.currentRecord = new ArrayRecord(requiredSchema.getColumns().toArray(new Column[0]));
        this.partitionValues = new ArrayList<>(partitionKeys.size());
        List<Column> requiredPartitionColumns = requiredSchema.getColumns()
                .stream()
                .filter(col -> partitionKeys.contains(col.getName()))
                .collect(Collectors.toList());
        for (Column requiredPartitionColumn : requiredPartitionColumns) {
            String partitionValue = inputSplit.getPartitionSpec().get(requiredPartitionColumn.getName());
            switch (requiredPartitionColumn.getTypeInfo().getOdpsType()) {
                case TINYINT: {
                    partitionValues.add(Byte.parseByte(partitionValue));
                    break;
                }
                case INT: {
                    partitionValues.add(Integer.parseInt(partitionValue));
                    break;
                }
                case BIGINT: {
                    partitionValues.add(Long.parseLong(partitionValue));
                    break;
                }
                case SMALLINT: {
                    partitionValues.add(Short.parseShort(partitionValue));
                    break;
                }
                case VARCHAR: {
                    VarcharTypeInfo varcharType = (VarcharTypeInfo) requiredPartitionColumn.getTypeInfo();
                    partitionValues.add(new Varchar(partitionValue, varcharType.getLength()));
                    break;
                }
                case STRING: {
                    partitionValues.add(partitionValue);
                    break;
                }
                case CHAR: {
                    CharTypeInfo charType = (CharTypeInfo) requiredPartitionColumn.getTypeInfo();
                    partitionValues.add(new Char(partitionValue, charType.getLength()));
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported odps type:" +
                            requiredPartitionColumn.getTypeInfo().getOdpsType());
                }
            }
        }
    }
}
