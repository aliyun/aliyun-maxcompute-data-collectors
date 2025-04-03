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
package io.trino.plugin.maxcompute;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.trino.plugin.maxcompute.utils.MaxComputeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MaxComputeSplitManager
        implements ConnectorSplitManager
{
    private final Odps odps;
    private final EnvironmentSettings settings;

    @Inject
    public MaxComputeSplitManager(MaxComputeConfig config)
    {
        this.odps = MaxComputeUtils.getOdps(requireNonNull(config, "config is null"));
        this.settings = MaxComputeUtils.getEnvironmentSettings(requireNonNull(config, "config is null"));
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        try {
            MaxComputeTableHandle maxComputeTableHandle = (MaxComputeTableHandle) tableHandle;
            Table table = odps.tables().get(maxComputeTableHandle.getProjectId(), maxComputeTableHandle.getSchemaName(), maxComputeTableHandle.getTableName());

            TableReadSessionBuilder tableReadSessionBuilder =
                    new TableReadSessionBuilder().identifier(TableIdentifier.of(table.getProject(), table.getSchemaName(), table.getName()))
                            .withSettings(settings);

            Optional<List<ColumnHandle>> projectedColumns = maxComputeTableHandle.getProjectedColumns();
            projectedColumns.ifPresent(columnHandles ->
                    tableReadSessionBuilder.requiredDataColumns(columnHandles.stream().map(e -> ((MaxComputeColumnHandle) e).getName()).collect(Collectors.toList())));
            if (table.isPartitioned()) {
                tableReadSessionBuilder.requiredPartitions(extractPartition(table, maxComputeTableHandle.getConstraint()));
            }

            TableBatchReadSession readSession = tableReadSessionBuilder.buildBatchReadSession();
            List<MaxComputeSplit> splits = Arrays.stream(readSession.getInputSplitAssigner().getAllSplits()).map(e -> {
                MaxComputeInputSplit maxComputeInputSplit = new MaxComputeInputSplit(e);
                return new MaxComputeSplit(maxComputeInputSplit, readSession, Collections.emptyMap());
            }).collect(Collectors.toList());
            return new FixedSplitSource(splits);
        }
        catch (OdpsException e) {
            throw MaxComputeUtils.wrapOdpsException(e);
        }
        catch (IOException e) {
            throw new TrinoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, e);
        }
    }

    private List<PartitionSpec> extractPartition(Table table, TupleDomain<ColumnHandle> constraint)
            throws OdpsException
    {
        List<PartitionSpec> res = new ArrayList<>();
        List<PartitionSpec> allPartitions = table.getPartitionSpecs();
        Optional<Map<ColumnHandle, Domain>> domains = constraint.getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            Map<String, Domain> columnConstant = columnHandleDomainMap.entrySet().stream().collect(Collectors.toMap(k -> ((MaxComputeColumnHandle) k.getKey()).getName(), k -> k.getValue()));

            for (PartitionSpec partition : allPartitions) {
                if (validateColumnValues(columnConstant, partition)) {
                    res.add(partition);
                }
            }
        }
        else {
            // If there is no constraint, default scan all partitions
            return allPartitions;
        }
        return res;
    }

    public static boolean validateColumnValues(Map<String, Domain> columnConstant, PartitionSpec partition)
    {
        Set<String> partitionColumns = partition.keys();
        for (String partitionColumn : partitionColumns) {
            if (!columnConstant.containsKey(partitionColumn)) {
                continue;
            }
            Domain domain = columnConstant.get(partitionColumn);
            Type columnType = domain.getType();

            String valueString = partition.get(partitionColumn);
            Object value;
            if (columnType instanceof BigintType) {
                value = Long.parseLong(valueString);
            }
            else if (columnType instanceof IntegerType) {
                value = Integer.parseInt(valueString);
            }
            else if (columnType instanceof DoubleType) {
                value = Double.parseDouble(valueString);
            }
            else if (columnType instanceof VarcharType) {
                value = Slices.utf8Slice(valueString);
            }
            else if (columnType instanceof BooleanType) {
                value = Boolean.parseBoolean(valueString);
            }
            else {
                throw new TrinoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR,
                        "Unsupported partition column type: " + columnType.getDisplayName());
            }
            if (!domain.includesNullableValue(value)) {
                return false;
            }
        }
        return true;
    }
}
