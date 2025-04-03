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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.table.TableIdentifier;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * indicate the real table handle of maxcompute.
 * connector id indicate the region and the project of the table.
 * the schema name could be null when maxcompute project don't support schema model.
 */
public class MaxComputeTableHandle
        implements ConnectorTableHandle
{
    private final String projectId;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<List<ColumnHandle>> projectedColumns;
    private final OptionalLong limit;

    @JsonCreator
    public MaxComputeTableHandle(
            @JsonProperty("projectId") String projectId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("projectedColumns") Optional<List<ColumnHandle>> projectedColumns,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.projectId = requireNonNull(projectId, "projectId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.projectedColumns = requireNonNull(projectedColumns, "projectedColumns is null");
        this.limit = requireNonNull(limit, "limit is null");
    }

    public MaxComputeTableHandle(String projectId, String schemaName, String tableName)
    {
        this(projectId, schemaName, tableName, TupleDomain.all(), Optional.empty(), OptionalLong.empty());
    }

    @JsonProperty
    public String getProjectId()
    {
        return projectId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<List<ColumnHandle>> getProjectedColumns()
    {
        return projectedColumns;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaxComputeTableHandle that = (MaxComputeTableHandle) o;
        return Objects.equals(projectId, that.projectId) &&
                Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(projectedColumns, that.projectedColumns) &&
                Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(projectId, schemaName, tableName, constraint, projectedColumns, limit);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projectId", projectId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("constraint", constraint)
                .add("projectedColumns", projectedColumns)
                .add("limit", limit)
                .toString();
    }

    public TableIdentifier getTableId()
    {
        return TableIdentifier.of(projectId, schemaName, tableName);
    }

    MaxComputeTableHandle withConstraint(TupleDomain<ColumnHandle> newConstraint)
    {
        return new MaxComputeTableHandle(projectId, schemaName, tableName, newConstraint, projectedColumns, limit);
    }

    MaxComputeTableHandle withProjectedColumns(List<ColumnHandle> newProjectedColumns)
    {
        return new MaxComputeTableHandle(projectId, schemaName, tableName, constraint, Optional.of(newProjectedColumns), limit);
    }
}
