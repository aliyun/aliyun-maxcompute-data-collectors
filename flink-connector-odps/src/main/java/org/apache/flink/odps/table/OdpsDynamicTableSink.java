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

package org.apache.flink.odps.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.output.OdpsOutputFormat;
import org.apache.flink.odps.output.OdpsSinkFunction;
import org.apache.flink.odps.output.stream.*;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.stream.Collectors;

public class OdpsDynamicTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsDynamicTableSink.class);

    private final OdpsConf odpsConf;
    private final ReadableConfig flinkConf;
    private final OdpsTablePath identifier;
    private final TableSchema tableSchema;
    private final List<String> partitionKeys;
    private final OdpsWriteOptions writeOptions;
    private LinkedHashMap<String, String> staticPartitionSpec = new LinkedHashMap<>();
    private boolean overwrite = false;
    private boolean dynamicGrouping = false;

    @Nullable
    private final Integer configuredParallelism;

    public OdpsDynamicTableSink(
            ReadableConfig flinkConf,
            OdpsConf odpsConf,
            OdpsWriteOptions writeOptions,
            OdpsTablePath identifier,
            TableSchema schema,
            List<String> partitionKeys,
            @Nullable Integer configuredParallelism) {
        this.flinkConf = flinkConf;
        this.odpsConf = odpsConf;
        this.writeOptions = writeOptions;
        this.identifier = identifier;
        this.tableSchema = schema;
        this.partitionKeys = partitionKeys;
        this.configuredParallelism = configuredParallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter =
                context.createDataStructureConverter(tableSchema.toRowDataType());
        return (DataStreamSinkProvider)
                dataStream -> consume(dataStream, context.isBounded(), converter);
    }

    private DataStreamSink<?> consume(
            DataStream<RowData> dataStream, boolean isBounded, DataStructureConverter converter) {
        final int parallelism =
                Optional.ofNullable(configuredParallelism).orElse(dataStream.getParallelism());
        if (isBounded) {
            return dataStream
                    .map((MapFunction<RowData, Row>) value -> (Row) converter.toExternal(value))
                    .writeUsingOutputFormat(createOdpsOutputFormat())
                    .setParallelism(parallelism);
        } else {
            if (overwrite) {
                throw new IllegalStateException("Streaming mode not support overwrite.");
            }
            final SinkFunction<Row> odpsProducer = createOdpsProducer();
            return dataStream
                    .map((MapFunction<RowData, Row>) value -> (Row) converter.toExternal(value))
                    .addSink(odpsProducer)
                    .setParallelism(parallelism);
        }
    }

    @Override
    public DynamicTableSink copy() {
        OdpsDynamicTableSink sink =
                new OdpsDynamicTableSink(
                        flinkConf,
                        odpsConf,
                        writeOptions,
                        identifier,
                        tableSchema,
                        partitionKeys,
                        configuredParallelism);
        sink.staticPartitionSpec = staticPartitionSpec;
        sink.overwrite = overwrite;
        sink.dynamicGrouping = dynamicGrouping;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "OdpsSink";
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // make it a LinkedHashMap to maintain partition column order
        if (partitionKeys != null
                && partitionKeys.size() != 0) {
            staticPartitionSpec = new LinkedHashMap<>();
            for (String partitionCol : partitionKeys) {
                if (partition.containsKey(partitionCol)) {
                    staticPartitionSpec.put(partitionCol, partition.get(partitionCol));
                }
            }
        } else {
            throw new UnsupportedOperationException(
                    "Should not apply partitions to a non-partitioned table.");
        }
    }

    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        this.dynamicGrouping = supportsGrouping;
        return supportsGrouping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OdpsDynamicTableSink)) {
            return false;
        }
        OdpsDynamicTableSink that = (OdpsDynamicTableSink) o;
        return Objects.equals(odpsConf, that.odpsConf)
                && Objects.equals(flinkConf, that.flinkConf)
                && Objects.equals(writeOptions, that.writeOptions)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(staticPartitionSpec, that.staticPartitionSpec)
                && overwrite == that.overwrite
                && dynamicGrouping == that.dynamicGrouping
                && Objects.equals(configuredParallelism, that.configuredParallelism);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                odpsConf,
                flinkConf,
                identifier,
                tableSchema,
                partitionKeys,
                writeOptions,
                staticPartitionSpec,
                overwrite,
                dynamicGrouping,
                configuredParallelism);
    }


    private SinkFunction<Row> createOdpsProducer() {
        boolean isPartitioned = partitionKeys != null && !partitionKeys.isEmpty();
        boolean isDynamicPartition = isPartitioned && partitionKeys.size() > staticPartitionSpec.size();
        boolean supportPartitionGrouping = dynamicGrouping;
        String projectName = identifier.getProjectName();
        String tableName = identifier.getTableName();
        String partition = "";
        if (isPartitioned) {
            validatePartitionSpec();
            partition = staticPartitionSpec
                    .keySet()
                    .stream()
                    .map(colName -> colName + "=" + staticPartitionSpec.get(colName))
                    .collect(Collectors.joining(","));
        }
        if (odpsConf.isClusterMode()) {
            //TODO: support file sink function for cluster mode
            throw new UnsupportedOperationException();
        } else {
            OdpsSinkFunction.OdpsSinkBuilder<Row> builder =
                    new OdpsSinkFunction.OdpsSinkBuilder<>(odpsConf, projectName, tableName);
            builder.setPartition(partition);
            builder.setDynamicPartition(isDynamicPartition);
            builder.setSupportPartitionGrouping(supportPartitionGrouping);
            if (isDynamicPartition) {
                PartitionComputer<Row> partitionComputer = new PartitionComputer<>(
                        writeOptions.getDynamicPartitionDefaultValue(),
                        tableSchema.getTableColumns()
                                .stream()
                                .map(TableColumn::getName)
                                .collect(Collectors.toList()),
                        partitionKeys,
                        partition);
                PartitionAssigner<Row> partitionAssigner = new TablePartitionAssigner<>(partitionComputer);
                builder.setPartitionAssigner(partitionAssigner);

                if (!StringUtils.isNullOrWhitespaceOnly(writeOptions.getDynamicPartitionAssignerClass())) {
                    try {
                        Class clz = Class.forName(writeOptions.getDynamicPartitionAssignerClass());
                        Constructor<PartitionAssigner> constructor =
                                (Constructor<PartitionAssigner>) clz.getDeclaredConstructor(new Class[]{});
                        constructor.setAccessible(true);
                        builder.setPartitionAssigner(constructor.newInstance());
                    } catch (Exception e) {
                        LOG.error("PartitionAssigner initialized failed: " + e.toString());
                    }
                }
            }
            builder.setWriteOptions(writeOptions);
            return builder.build();
        }
    }

    private OdpsOutputFormat<Row> createOdpsOutputFormat() {
        boolean isPartitioned = partitionKeys != null && !partitionKeys.isEmpty();
        boolean isDynamicPartition = isPartitioned && partitionKeys.size() > staticPartitionSpec.size();
        boolean supportPartitionGrouping = dynamicGrouping;
        String projectName = identifier.getProjectName();
        String tableName = identifier.getTableName();
        String partition = "";
        if (isPartitioned) {
            validatePartitionSpec();
            partition = staticPartitionSpec
                    .keySet()
                    .stream()
                    .map(colName -> colName + "=" + staticPartitionSpec.get(colName))
                    .collect(Collectors.joining(","));
        }
        OdpsOutputFormat.OutputFormatBuilder<Row> builder =
                new OdpsOutputFormat.OutputFormatBuilder<>(odpsConf, projectName, tableName);
        builder.setOverwrite(overwrite);
        builder.setPartition(partition);
        builder.setDynamicPartition(isDynamicPartition);
        builder.setSupportPartitionGrouping(supportPartitionGrouping);
        builder.setWriteOptions(writeOptions);
        return builder.build();
    }

    private void validatePartitionSpec() {
        List<String> unknownPartCols = staticPartitionSpec
                .keySet()
                .stream()
                .filter(k -> !partitionKeys.contains(k))
                .collect(Collectors.toList());
        Preconditions.checkArgument(
                unknownPartCols.isEmpty(),
                "Static partition spec contains unknown partition column: " + unknownPartCols.toString());
        int numStaticPart = staticPartitionSpec.size();
        if (numStaticPart < partitionKeys.size()) {
            for (String partitionCol : partitionKeys) {
                if (!staticPartitionSpec.containsKey(partitionCol)) {
                    // this is a dynamic partition, make sure we have seen all static ones
                    Preconditions.checkArgument(numStaticPart == 0,
                            "Dynamic partition cannot appear before static partition");
                    return;
                } else {
                    numStaticPart--;
                }
            }
        }
    }
}
