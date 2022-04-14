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

package org.apache.flink.odps.input;

import com.aliyun.odps.*;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.aliyun.odps.cupid.table.v1.util.Options;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.input.reader.CupidBatchIterator;
import org.apache.flink.odps.input.reader.NextIterator;
import org.apache.flink.odps.input.reader.RecordIterator;
import org.apache.flink.odps.schema.OdpsColumn;
import org.apache.flink.odps.schema.OdpsTableSchema;
import org.apache.flink.odps.util.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.odps.util.Constants.*;
import static org.apache.flink.odps.util.OdpsUtils.RecordType;
import static org.apache.flink.odps.util.OdpsUtils.getPartitionSpecKVMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class OdpsInputFormat<T> extends RichInputFormat<T, OdpsInputSplit>
        implements ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsInputFormat.class);
    private static final long serialVersionUID = 1L;

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private transient Odps odps;

    private String[] partitions;
    private final String[] columns;
    private final int numPartitions;
    private final int splitSize;

    private String[] selectedColumns;
    private boolean isPartitioned;
    private final OdpsTableSchema odpsTableSchema;
    private TypeInformation resultType;

    private transient NextIterator<T> rowIterator;
    private Class<?> pojoClass;

    private String tableApiProvider = Constants.DEFAULT_TABLE_API_PROVIDER;
    private boolean isLocal = false;
    private boolean useBatch = true;

    private transient OdpsMetaDataProvider tableMetaProvider;
    protected RecordType recordType = RecordType.FLINK_ROW_DATA;

    public OdpsInputFormat(String project, String table) {
        this(null, project, table);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table) {
        this(odpsConf, project, table, null, null, 0, 0);
    }

    public OdpsInputFormat(String project, String table, int splitSize) {
        this(null, project, table, splitSize);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, int splitSize) {
        this(odpsConf, project, table, null, null, 0, splitSize);
    }

    public OdpsInputFormat(String project, String table, String[] columns) {
        this(null, project, table, columns);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns) {
        this(odpsConf, project, table, columns, 0);
    }

    public OdpsInputFormat(String project, String table, String[] columns, int splitSize) {
        this(null, project, table, columns, null, 0, splitSize);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns, int splitSize) {
        this(odpsConf, project, table, columns, null, 0, splitSize);
    }

    public OdpsInputFormat(String project, String table, String[] columns, String[] partitions) {
        this(null, project, table, columns, partitions);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns, String[] partitions) {
        this(odpsConf, project, table, columns, partitions, 0);
    }

    public OdpsInputFormat(String project, String table, String[] columns, String[] partitions, int splitSize) {
        this(null, project, table, columns, partitions, 0, splitSize);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns, String[] partitions, int splitSize) {
        this(odpsConf, project, table, columns, partitions, 0, splitSize);
    }

    public OdpsInputFormat(OdpsConf odpsConf, String project, String table, String[] columns, String[] partitions, int numPartitions, int splitSize) {
        Preconditions.checkNotNull(table, "table cannot be null");
        Preconditions.checkNotNull(project, "project cannot be null");

        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }

        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");
        if (!this.odpsConf.isClusterMode()) {
            this.isLocal = true;
            this.tableApiProvider = TUNNEL_TABLE_API_PROVIDER;
            this.useBatch = false;
        }
        this.projectName = project;
        this.tableName = table;
        this.tableMetaProvider = new OdpsMetaDataProvider(this.getOdps());
        this.odpsTableSchema = getOdpsTableSchema();
        this.partitions = partitions;
        this.initPartitions();
        this.columns = columns;
        this.initSelectColumns();
        this.numPartitions = numPartitions;
        this.splitSize = splitSize > 0 ? splitSize :
                this.odpsConf.getPropertyOrDefault(ODPS_INPUT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE);
        LOG.info("Create odps input, table:{}.{},partitions:{},columns:{},splitSize:{}",
                this.projectName, this.tableName, this.partitions, this.columns, this.splitSize);
    }

    private Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    public OdpsInputFormat<T> setOdpsConf(OdpsConf odpsConf) {
        return setOdpsConf(odpsConf, false);
    }

    public OdpsInputFormat<T> setOdpsConf(OdpsConf odpsConf, boolean forceLocalRun) {
        if (odpsConf != null) {
            odpsConf.setClusterMode(!forceLocalRun);
            OdpsInputFormatBuilder<T> builder = new OdpsInputFormatBuilder<>(odpsConf, projectName, tableName);
            builder.setColumns(columns);
            builder.setPartitions(partitions);
            builder.setNumPartitions(numPartitions);
            builder.setSplitSize(splitSize);
            return builder.build();
        }
        return this;
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public OdpsInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
        if (isPartitioned && partitions.length == 0) {
            return new OdpsInputSplit[0];
        }
        TableReadSession tableReadSession;
        InputSplit[] inputSplits;
        OdpsInputSplit[] odpsInputSplits;
        try {
            List<Attribute> reqColumns = Arrays.stream(selectedColumns)
                    .filter(columnName -> columnName != null && !odpsTableSchema.isPartitionColumn(columnName))
                    .map(odpsTableSchema::getColumn)
                    .map(col -> new Attribute(col.getName(), col.getTypeName()))
                    .collect(Collectors.toList());
            // tunnel request 0 column default
            if (reqColumns.size() == 0) {
                OdpsColumn odpsColumn = odpsTableSchema.getColumns().get(0);
                reqColumns.add(0, new Attribute(odpsColumn.getName(), odpsColumn.getTypeName()));
            }
            RequiredSchema requiredSchema = RequiredSchema.columns(reqColumns);
            Options options = OdpsUtils.getOdpsOptions(odpsConf);
            if (partitions.length > 0) {
                List<PartitionSpecWithBucketFilter> partitionSpecWithBucketFilterList = Arrays.stream(partitions)
                        .map(e -> new PartitionSpecWithBucketFilter(getPartitionSpecKVMap(new PartitionSpec(e))))
                        .collect(Collectors.toList());
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, projectName, tableName)
                        .readPartitions(partitionSpecWithBucketFilterList)
                        .readDataColumns(requiredSchema)
                        .options(options)
                        .build();
            } else {
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, projectName, tableName)
                        .readDataColumns(requiredSchema)
                        .options(options)
                        .build();
            }
            inputSplits = tableReadSession.getOrCreateInputSplits(splitSize);
        } catch (Exception e) {
            throw new IOException("create table read session failed", e);
        }
        odpsInputSplits = new OdpsInputSplit[inputSplits.length];
        for (int i = 0; i < odpsInputSplits.length; i++) {
            InputSplit split = inputSplits[i];
            odpsInputSplits[i] = new OdpsInputSplit(split, i);
        }
        LOG.info("get input splits size: " + odpsInputSplits.length);
        return odpsInputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(OdpsInputSplit[] odpsInputSplits) {
        return new DefaultInputSplitAssigner(odpsInputSplits);
    }

    @Override
    public void open(OdpsInputSplit split) throws IOException {
        if (split == null) {
            return;
        }
        LOG.info("open inputFormat: " + split.getSplitNumber());
        try {
            if (useBatch) {
                useBatch = supportBatch(odpsTableSchema, selectedColumns);
            }
            if (!useBatch) {
                rowIterator = new RecordIterator<>(split,
                        odpsTableSchema,
                        selectedColumns,
                        recordType);
            } else {
                rowIterator = new CupidBatchIterator<>(split,
                        odpsTableSchema,
                        selectedColumns,
                        recordType,
                        odpsConf.getPropertyOrDefault(ODPS_VECTORIZED_BATCH_SIZE, DEFAULT_ODPS_VECTORIZED_BATCH_SIZE));
            }
        } catch (Exception e) {
            throw new IOException("create table reader failed", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (rowIterator == null) {
            return true;
        }
        return !rowIterator.hasNext();
    }

    @Override
    public T nextRecord(T reuse) throws IOException {
        rowIterator.setReuse(reuse);
        return rowIterator.next();
    }

    @Override
    public void close() throws IOException {
        if (rowIterator != null) {
            rowIterator.close();
        }
        rowIterator = null;
        LOG.info("close odpsInputFormat");
    }

    @Override
    public TypeInformation<T> getProducedType() {
        if (this.resultType == null) {
            int numFields = selectedColumns.length;
            if (recordType == RecordType.FLINK_TUPLE && numFields > MAX_FLINK_TUPLE_SIZE) {
                throw new IllegalArgumentException("Only up to " + MAX_FLINK_TUPLE_SIZE +
                        " fields can be returned as Flink tuples.");
            }
            TypeInformation[] fieldTypes;
            DataType[] dataTypes = new DataType[numFields];
            int fieldPos = 0;
            for (String columnName : this.selectedColumns) {
                if (odpsTableSchema.getColumn(columnName) == null) {
                    throw new IllegalArgumentException("column " + columnName + " not exist in table");
                }
                dataTypes[fieldPos] = OdpsTypeUtil.toFlinkType(odpsTableSchema.getColumn(columnName).getTypeInfo());
                fieldPos++;
            }
            if (this.recordType == RecordType.FLINK_ROW_DATA) {
                this.resultType = getProducedType(dataTypes);
            } else {
                fieldTypes = TypeConversions.fromDataTypeToLegacyInfo(dataTypes);
                if (this.recordType == RecordType.FLINK_ROW) {
                    this.resultType = new RowTypeInfo(fieldTypes);
                } else if (this.recordType == RecordType.FLINK_TUPLE) {
                    this.resultType = new TupleTypeInfo(fieldTypes);
                } else if (this.recordType == RecordType.POJO) {
                    this.resultType = TypeExtractor.getForClass(pojoClass);
                }
            }
        }
        return resultType;
    }

    private TypeInformation<RowData> getProducedType(DataType[] dataTypes) {
        org.apache.flink.table.api.TableSchema fullSchema = org.apache.flink.table.api.TableSchema.builder()
                .fields(this.selectedColumns, dataTypes)
                .build();
        return InternalTypeInfo.of(fullSchema.toRowDataType().getLogicalType());
    }

    public OdpsInputFormat<T> asFlinkTuples() {
        recordType = RecordType.FLINK_TUPLE;
        return this;
    }

    public OdpsInputFormat<T> asFlinkRows() {
        recordType = RecordType.FLINK_ROW;
        return this;
    }

    public OdpsInputFormat<T> asFlinkRowData() {
        recordType = RecordType.FLINK_ROW_DATA;
        return this;
    }

    public OdpsInputFormat<T> asPojos(Class<?> clazz) {
        recordType = RecordType.POJO;
        pojoClass = clazz;
        return this;
    }

    private void initPartitions() {
        if (isPartitioned) {
            if (this.partitions == null) {
                // when not specific partition, get all
                Table t = this.tableMetaProvider.getTable(projectName, tableName);
                List<Partition> partitionList = t.getPartitions();
                this.partitions = OdpsUtils.createPartitionSpec(partitionList);
            }
        } else {
            this.partitions = new String[0];
        }
        this.checkPartitions();
    }

    private void checkPartitions() {
        Table t = this.tableMetaProvider.getTable(projectName, tableName);
        for (String p : partitions) {
            PartitionSpec partitionSpec = new PartitionSpec(p);
            try {
                if (!t.hasPartition(partitionSpec)) {
                    throw new FlinkOdpsException("partition not exist. " + partitionSpec.toString());
                }
            } catch (OdpsException e) {
                throw new FlinkOdpsException("check partition failed!", e);
            }
        }
    }

    private void initSelectColumns() {
        if (columns == null) {
            // when not specific column, get all (contains partition columns)
            this.selectedColumns = this.odpsTableSchema.getColumns()
                    .stream()
                    .map(OdpsColumn::getName)
                    .toArray(String[]::new);
        } else {
            // no need to add partition column
            List<String> columnList = new ArrayList<>(Arrays.asList(columns));
            columnList.forEach(column -> {
                if (odpsTableSchema.getColumn(column.toLowerCase()) == null) {
                    LOG.error("invalid column: " + column);
                    throw new FlinkOdpsException("column " + column + " not exist in table: " + tableName);
                }
            });

            this.selectedColumns = columnList
                    .stream()
                    .map(String::toLowerCase)
                    .distinct()
                    .toArray(String[]::new);
        }
    }

    private OdpsTableSchema getOdpsTableSchema() {
        Table t = this.tableMetaProvider.getTable(projectName, tableName);
        TableSchema schema = t.getSchema();
        this.isPartitioned = schema.getPartitionColumns().size() > 0;
        return new OdpsTableSchema(schema.getColumns(), schema.getPartitionColumns(), t.isVirtualView());
    }

    private boolean supportBatch(OdpsTableSchema tableSchema, String[] selectedColumns) {
        if (isLocal || !odpsConf.getPropertyOrDefault(ODPS_VECTORIZED_READ_ENABLE, false)) {
            return false;
        }
        for (String columnName : selectedColumns) {
            OdpsColumn column = tableSchema.getColumn(columnName);
            if (!column.getType().equals(OdpsType.BOOLEAN) &&
                    !column.getType().equals(OdpsType.STRING) &&
                    !column.getType().equals(OdpsType.CHAR) &&
                    !column.getType().equals(OdpsType.VARCHAR) &&
                    !column.getType().equals(OdpsType.BINARY) &&
                    !column.getType().equals(OdpsType.DECIMAL) &&
                    !column.getType().equals(OdpsType.DATETIME) &&
                    !column.getType().equals(OdpsType.DATE) &&
                    !column.getType().equals(OdpsType.TIMESTAMP) &&
                    !column.getType().equals(OdpsType.DOUBLE) &&
                    !column.getType().equals(OdpsType.FLOAT) &&
                    !column.getType().equals(OdpsType.BIGINT) &&
                    !column.getType().equals(OdpsType.INT) &&
                    !column.getType().equals(OdpsType.TINYINT) &&
                    !column.getType().equals(OdpsType.SMALLINT)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Builder to build {@link OdpsInputFormat}.
     */
    public static class OdpsInputFormatBuilder<T> {

        private final OdpsConf odpsConf;
        private final String projectName;
        private final String tableName;
        private String[] partitions;
        private String[] columns;
        private int numPartitions = 0;
        private int splitSize =  0;

        public OdpsInputFormatBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OdpsInputFormatBuilder(OdpsConf odpsConf, String projectName, String tableName) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
        }

        public OdpsInputFormatBuilder<T> setColumns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public OdpsInputFormatBuilder<T> setPartitions(String[] partitions) {
            this.partitions = partitions;
            return this;
        }

        public OdpsInputFormatBuilder<T> setNumPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public OdpsInputFormatBuilder<T> setSplitSize(int splitSize) {
            this.splitSize = splitSize;
            return this;
        }

        public OdpsInputFormat<T> build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");
            return new OdpsInputFormat<>(
                    odpsConf,
                    projectName,
                    tableName,
                    columns,
                    partitions,
                    numPartitions,
                    splitSize
            );
        }
    }
}

