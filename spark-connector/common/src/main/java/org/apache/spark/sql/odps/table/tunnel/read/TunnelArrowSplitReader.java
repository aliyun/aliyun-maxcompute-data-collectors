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
import com.aliyun.odps.data.ArrowRecordReader;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.table.read.SplitReader;
import com.aliyun.odps.table.utils.SchemaUtils;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TunnelArrowSplitReader implements SplitReader<VectorSchemaRoot> {

    private ArrowRecordReader reader;
    private boolean isClosed;
    private final TunnelInputSplit inputSplit;
    private final ReaderOptions readerOptions;

    private boolean hasPartitionColumn;
    private List<Column> requiredPartitionColumns;
    private List<Field> partitionFields;
    private List<Object> partitionValues;

    private boolean hasDataColumn;

    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    private VectorSchemaRoot cache;

    public TunnelArrowSplitReader(TableIdentifier identifier,
                                  TunnelInputSplit split,
                                  DataSchema dataSchema,
                                  ReaderOptions options) throws IOException {
        this.isClosed = false;
        this.inputSplit = split;
        this.readerOptions = options;
        init(identifier, split, dataSchema, options);
        initMetrics();
    }

    @Override
    public boolean hasNext() throws IOException {
        boolean hasNext = cache != null;

        if (!hasNext) {
            if (hasDataColumn) {
                cache = reader.read();
                hasNext = cache != null;
            } else {
                // only has partition column
                hasNext = recordCount.getCount() < inputSplit.getRowRange().getNumRecord();
            }

            if (hasNext && hasPartitionColumn) {
                int rowCount;
                if (hasDataColumn) {
                    rowCount = cache.getRowCount();
                } else {
                    // only has partition column
                    rowCount = (int)inputSplit.getRowRange().getNumRecord();
                }

                List<Field> fields = hasDataColumn ?
                        new ArrayList<>(cache.getSchema().getFields()) : new ArrayList<>();
                List<FieldVector> vectors = hasDataColumn ?
                        new ArrayList<>(cache.getFieldVectors()) : new ArrayList<>();

                List<FieldVector> partitionVectors = partitionFields.stream()
                        .map(field -> field.createVector(readerOptions.getBufferAllocator()))
                        .collect(Collectors.toList());
                setPartitionVectors(partitionVectors, rowCount);

                fields.addAll(partitionFields);
                vectors.addAll(partitionVectors);

                cache = new VectorSchemaRoot(fields, vectors);
                cache.setRowCount(rowCount);
            }
        }
        return hasNext;
    }

    @Override
    public VectorSchemaRoot get() {
        VectorSchemaRoot result = cache;
        if (result != null) {
            recordCount.inc(result.getRowCount());
            cache = null;
        }
        return result;
    }

    @Override
    public Metrics currentMetricsValues() {
        if (hasDataColumn) {
            bytesCount.setValue(reader.bytesRead());
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
                .filter(col -> !partitionKeys.contains(col.getName()))
                .collect(Collectors.toList());

        this.hasDataColumn = readDataColumns.size() > 0;
        if (hasDataColumn) {
            // if data columns size > 0, then create reader
            try {
                TableTunnel tunnel = TableUtils.getTableTunnel(options.getSettings());
                TableTunnel.DownloadSession session;
                if (partitionSpec == null || partitionSpec.isEmpty()) {
                    session = tunnel.getDownloadSession(project, table, downloadId);
                } else {
                    session = tunnel.getDownloadSession(project, table, partitionSpec, downloadId);
                }
                this.reader = session.openArrowRecordReader(startIndex, numRecord, readDataColumns);
            } catch (TunnelException e) {
                throw new IOException(e);
            }
        }

        this.requiredPartitionColumns = requiredSchema.getColumns()
                .stream()
                .filter(col -> partitionKeys.contains(col.getName()))
                .collect(Collectors.toList());
        this.partitionFields = requiredPartitionColumns.stream()
                .map(SchemaUtils::columnToArrowField)
                .collect(Collectors.toList());
        this.hasPartitionColumn = requiredPartitionColumns.size() > 0;
        this.partitionValues = new ArrayList<>(requiredPartitionColumns.size());
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
                case CHAR:
                case VARCHAR:
                case STRING: {
                    partitionValues.add(partitionValue);
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported odps type:" +
                            requiredPartitionColumn.getTypeInfo().getOdpsType());
                }
            }
        }
    }

    private void setPartitionVectors(List<FieldVector> partitionVectors,
                                     int rowCount) {
        for (int i = 0; i < partitionVectors.size(); i++) {
            partitionVectors.get(i).allocateNew();
            Object partitionValue = partitionValues.get(i);
            switch (requiredPartitionColumns.get(i).getTypeInfo().getOdpsType()) {
                case TINYINT: {
                    for (int j = 0; j < rowCount; j++) {
                        ((TinyIntVector) (partitionVectors.get(i))).setSafe(j, (Byte) partitionValue);
                    }
                    break;
                }
                case INT: {
                    for (int j = 0; j < rowCount; j++) {
                        ((IntVector) (partitionVectors.get(i))).setSafe(j, (Integer) partitionValue);
                    }
                    break;
                }
                case BIGINT: {
                    for (int j = 0; j < rowCount; j++) {
                        ((BigIntVector) (partitionVectors.get(i))).setSafe(j, (Long) partitionValue);
                    }
                    break;
                }
                case SMALLINT: {
                    for (int j = 0; j < rowCount; j++) {
                        ((SmallIntVector) (partitionVectors.get(i))).setSafe(j, (Short) partitionValue);
                    }
                    break;
                }
                case VARCHAR:
                case STRING:
                case CHAR: {
                    for (int j = 0; j < rowCount; j++) {
                        ((VarCharVector) (partitionVectors.get(i))).setSafe(j, ((String) partitionValue).getBytes());
                    }
                    break;
                }
            }
        }
        partitionVectors.forEach(vec -> vec.setValueCount(rowCount));
    }
}