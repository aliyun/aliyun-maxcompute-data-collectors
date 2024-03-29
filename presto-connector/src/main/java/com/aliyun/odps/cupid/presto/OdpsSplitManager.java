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
package com.aliyun.odps.cupid.presto;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.reader.PartitionSpecWithBucketFilter;
import com.aliyun.odps.cupid.table.v1.reader.RequiredSchema;
import com.aliyun.odps.cupid.table.v1.reader.TableReadSession;
import com.aliyun.odps.cupid.table.v1.reader.TableReadSessionBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.commons.codec.binary.Base64;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OdpsSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final OdpsClient odpsClient;
    private final String tableApiProvider;
    private final int splitSize;

    @Inject
    public OdpsSplitManager(OdpsConnectorId connectorId, OdpsClient odpsClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.odpsClient = requireNonNull(odpsClient, "client is null");
        if (System.getenv("META_LOOKUP_NAME") != null) {
            tableApiProvider = "cupid-native";
        } else {
            tableApiProvider = "tunnel";
        }
        splitSize = odpsClient.getOdpsConfig().getSplitSize() <= 0 ? 256 : odpsClient.getOdpsConfig().getSplitSize();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        OdpsTableLayoutHandle layoutHandle = (OdpsTableLayoutHandle) layout;
        OdpsTable table = odpsClient.getTable(layoutHandle.getSchemaTableName().getSchemaName(),
                layoutHandle.getSchemaTableName().getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists",
                layoutHandle.getSchemaTableName().getSchemaName(), layoutHandle.getSchemaTableName().getTableName());

        Optional<List<OdpsPartition>> partitions = layoutHandle.getPartitions();
        List<ConnectorSplit> splits = new ArrayList<>();
        TableReadSession tableReadSession;
        InputSplit[] inputSplits;
        boolean isZeroColumn = false;
        try {
            Set<String> partitionColumnNames = layoutHandle.getPartitionColumns().stream().map(e -> e.getName()).collect(Collectors.toSet());
            List<Attribute> reqColumns = layoutHandle.getDesiredColumns().stream()
                    .filter(e -> !partitionColumnNames.contains(e.getName()))
                    .map(e -> new Attribute(e.getName(), OdpsUtils.toOdpsType(e.getType(), e.getIsStringType()).getTypeName())).collect(Collectors.toList());
            if (reqColumns.size() == 0) {
                // tunnel must set columns
                OdpsColumnHandle columnHandle = layoutHandle.getDataColumns().get(0);
                reqColumns.add(0, new Attribute(columnHandle.getName(),
                        OdpsUtils.toOdpsType(columnHandle.getType(), columnHandle.getIsStringType()).getTypeName()));
                isZeroColumn = true;
            }
            RequiredSchema requiredSchema = RequiredSchema.columns(reqColumns);
            if (table.getPartitionColumns().size() > 0) {
                List<PartitionSpecWithBucketFilter> partitionSpecWithBucketFilterList = partitions.get().stream()
                        .map(e -> new PartitionSpecWithBucketFilter(getPartitionSpecKVMap(e.getKeys())))
                        .collect(Collectors.toList());
                if (partitionSpecWithBucketFilterList.size() == 0) {
                    // no part specified
                    return new FixedSplitSource(ImmutableList.of());
                }
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, layoutHandle.getSchemaTableName().getSchemaName(),
                        layoutHandle.getSchemaTableName().getTableName())
                        .readPartitions(partitionSpecWithBucketFilterList)
                        .readDataColumns(requiredSchema)
                        .build();
            } else {
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, layoutHandle.getSchemaTableName().getSchemaName(),
                        layoutHandle.getSchemaTableName().getTableName())
                        .readDataColumns(requiredSchema)
                        .build();
            }
            inputSplits = tableReadSession.getOrCreateInputSplits(splitSize);
        } catch (Exception e) {
            throw new PrestoException(OdpsErrorCode.ODPS_INTERNAL_ERROR, "create TableReadSession failed!" + e.toString(), e);
        }

        for (InputSplit inputSplit : inputSplits) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(baos);
                out.writeObject(inputSplit);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String splitBase64Str = Base64.encodeBase64String(baos.toByteArray());
            splits.add(new OdpsSplit(connectorId, layoutHandle.getSchemaTableName().getSchemaName(),
                    layoutHandle.getSchemaTableName().getTableName(), splitBase64Str, isZeroColumn));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private Map<String, String> getPartitionSpecKVMap(Map<ColumnHandle, NullableValue> kvs) {
        Map<String, String> parts = new LinkedHashMap<>(2);
        for (Map.Entry<ColumnHandle, NullableValue> kv : kvs.entrySet()) {
            parts.put(((OdpsColumnHandle) kv.getKey()).getName(), ((Slice) kv.getValue().getValue()).toStringUtf8());
        }

        return parts;
    }
}
