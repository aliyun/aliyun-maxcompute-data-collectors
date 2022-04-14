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

import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.input.OdpsInputFormat;
import org.apache.flink.odps.input.OdpsLookupFunction;
import org.apache.flink.odps.input.OdpsLookupOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;


/**
 * A TableSource implementation to read data from Hive tables.
 */
public class OdpsDynamicTableSource
        implements ScanTableSource,
        LookupTableSource,
        SupportsPartitionPushDown,
        SupportsProjectionPushDown,
        SupportsLimitPushDown {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsDynamicTableSource.class);

    private final OdpsConf odpsConf;
    private final OdpsLookupOptions lookupOptions;
    private final ReadableConfig flinkConf;
    private final OdpsTablePath identifier;
    private final OdpsMetaDataProvider metaDataProvider;

    private TableSchema tableSchema;
    private final List<String> partitionKeys;

    // Remaining partition specs after partition pruning is performed. Null if pruning is not pushed
    // down.
    @Nullable
    private List<Map<String, String>> remainingPartitions = null;
    @Nullable
    protected int[] projectedFields = null;
    @Nullable
    private Long limit = null;

    public OdpsDynamicTableSource(
            ReadableConfig flinkConf,
            OdpsConf odpsConf,
            OdpsLookupOptions lookupOptions,
            OdpsTablePath identifier,
            TableSchema schema,
            List<String> partitionKeys) {
        this.flinkConf = flinkConf == null ? new Configuration() : flinkConf;
        this.odpsConf = Preconditions.checkNotNull(odpsConf, "odpsConf cannot be null");
        this.lookupOptions = lookupOptions;
        this.identifier = Preconditions.checkNotNull(identifier);
        this.tableSchema = schema;
        this.partitionKeys = partitionKeys;
        // TODO: Singleton
        this.metaDataProvider = new OdpsMetaDataProvider(OdpsUtils.getOdps(this.odpsConf));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        // only support non-nested look up keys
        int[] keyIndices = new int[context.getKeys().length];
        int i = 0;
        for (int[] key : context.getKeys()) {
            if (key.length > 1) {
                throw new UnsupportedOperationException(
                        "Hive lookup can not support nested key now.");
            }
            keyIndices[i] = key[0];
            i++;
        }
        return TableFunctionProvider.of(
                new OdpsLookupFunction(
                        getOdpsInputFormat(),
                        lookupOptions,
                        keyIndices,
                        (RowType) tableSchema.toRowDataType().getLogicalType()));
    }

    private OdpsInputFormat<RowData> getOdpsInputFormat() {
        OdpsInputFormat.OdpsInputFormatBuilder<RowData> builder =
                new OdpsInputFormat.OdpsInputFormatBuilder<>(odpsConf, this.identifier.getProjectName(),
                        this.identifier.getTableName());
        builder.setColumns(tableSchema.getFieldNames());
        builder.setPartitions(OdpsUtils.createPartitionSpec(getPrunedPartitions()));
        return builder.build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return InputFormatProvider.of(getOdpsInputFormat());
    }

    private List<Partition> getPrunedPartitions() {
        List<Partition> odpsPartitions = new ArrayList<>();
        if (partitionKeys != null && partitionKeys.size() > 0) {
            if (remainingPartitions != null) {
                for (Map<String, String> spec : remainingPartitions) {
                    PartitionSpec partitionSpec = new PartitionSpec();
                    spec.forEach(partitionSpec::set);
                    odpsPartitions.add(
                            metaDataProvider.getPartition(identifier.getProjectName(),
                                    identifier.getTableName(),
                                    partitionSpec.toString()));
                }
            } else {
                odpsPartitions.addAll(metaDataProvider.getPartitions(identifier.getProjectName(),
                        identifier.getTableName(),
                        true));
            }
        }
        return odpsPartitions;
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        // TODO: cache partitions
        if (partitionKeys != null && partitionKeys.size() > 0) {
            List<Map<String, String>> results = new ArrayList<>();
            List<Partition> partitions = metaDataProvider.getPartitions(identifier.getProjectName(),
                    identifier.getTableName());
            partitions.forEach(p -> {
                Map<String, String> part = new LinkedHashMap<>();
                partitionKeys.forEach(key -> {
                    part.put(key, p.getPartitionSpec().get(key));
                });
                results.add(part);
            });
            return Optional.of(results);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        if (partitionKeys != null
                && partitionKeys.size() != 0) {
            this.remainingPartitions = remainingPartitions;
        } else {
            throw new UnsupportedOperationException(
                    "Should not apply partitions to a non-partitioned table.");
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }

    @Override
    public String asSummaryString() {
        return "OdpsSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        OdpsDynamicTableSource source = new OdpsDynamicTableSource(
                flinkConf, odpsConf, lookupOptions, identifier, tableSchema, partitionKeys);
        source.remainingPartitions = remainingPartitions;
        source.projectedFields = projectedFields;
        source.limit = limit;
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OdpsDynamicTableSource)) {
            return false;
        }
        OdpsDynamicTableSource that = (OdpsDynamicTableSource) o;
        return Objects.equals(odpsConf, that.odpsConf)
                && Objects.equals(flinkConf, that.flinkConf)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(identifier, that.identifier)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(partitionKeys, that.partitionKeys)
                && Objects.equals(remainingPartitions, that.remainingPartitions)
                && Arrays.equals(projectedFields, that.projectedFields)
                && Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                odpsConf,
                flinkConf,
                lookupOptions,
                identifier,
                tableSchema,
                partitionKeys,
                remainingPartitions,
                projectedFields,
                limit);
    }
}
