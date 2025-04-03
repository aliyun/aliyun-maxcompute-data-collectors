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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MaxComputePartition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, ConstantExpression> keys;

    public MaxComputePartition(SchemaTableName tableName)
    {
        this(tableName, UNPARTITIONED_ID, ImmutableMap.of());
    }

    public MaxComputePartition(
            SchemaTableName tableName,
            String partitionId,
            Map<ColumnHandle, ConstantExpression> keys)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.partitionId = requireNonNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(requireNonNull(keys, "keys is null"));
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getPartitionId()
    {
        return partitionId;
    }

    public Map<ColumnHandle, ConstantExpression> getKeys()
    {
        return keys;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaxComputePartition other = (MaxComputePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }
}
