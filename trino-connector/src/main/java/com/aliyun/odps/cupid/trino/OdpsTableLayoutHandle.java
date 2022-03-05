/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.odps.cupid.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OdpsTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final List<OdpsColumnHandle> partitionColumns;
    private final List<OdpsColumnHandle> dataColumns;
    private final List<OdpsColumnHandle> desiredColumns;
    private final TupleDomain<ColumnHandle> partitionColumnPredicate;


    // coordinator-only properties
    @Nullable
    private final List<OdpsPartition> partitions;

    @JsonCreator
    public OdpsTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("dataColumns") List<OdpsColumnHandle> dataColumns,
            @JsonProperty("desiredColumns") List<OdpsColumnHandle> desiredColumns,
            @JsonProperty("partitionColumns") List<OdpsColumnHandle> partitionColumns,
            @JsonProperty("partitionColumnPredicate") TupleDomain<ColumnHandle> partitionColumnPredicate)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.desiredColumns = ImmutableList.copyOf(requireNonNull(desiredColumns, "desiredColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = null;
    }

    public OdpsTableLayoutHandle(
            SchemaTableName schemaTableName,
            List<OdpsColumnHandle> dataColumns,
            List<OdpsColumnHandle> desiredColumns,
            List<OdpsColumnHandle> partitionColumns,
            TupleDomain<ColumnHandle> partitionColumnPredicate,
            List<OdpsPartition> partitions)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.desiredColumns = ImmutableList.copyOf(requireNonNull(desiredColumns, "desiredColumns is null"));
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitionColumnPredicate = requireNonNull(partitionColumnPredicate, "partitionColumnPredicate is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonProperty
    public List<OdpsColumnHandle> getPartitionColumns() {
        return partitionColumns;
    }

    @JsonProperty
    public List<OdpsColumnHandle> getDataColumns() {
        return dataColumns;
    }

    @JsonProperty
    public List<OdpsColumnHandle> getDesiredColumns() {
        return desiredColumns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPartitionColumnPredicate() {
        return partitionColumnPredicate;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<List<OdpsPartition>> getPartitions()
    {
        return Optional.ofNullable(partitions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaTableName.getSchemaName())
                .add("tableName", schemaTableName.getTableName())
                .add("dataColumns", dataColumns)
                .add("partitionColumns", partitionColumns)
                .toString();
    }
}
