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

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Schema;
import com.aliyun.odps.Table;
import com.facebook.presto.maxcompute.utils.MaxComputeMetaCache;
import com.facebook.presto.maxcompute.utils.MaxComputeUtils;
import com.facebook.presto.maxcompute.utils.TypeConvertUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MaxComputeMetadata
        implements ConnectorMetadata
{
    private static final String DEFAULT_SCHEMA = "default";
    private final String connectorId;
    private final Odps odps;
    private final boolean supportSchema;
    private MaxComputeMetaCache metaCache;

    @Inject
    public MaxComputeMetadata(MaxComputeConnectorId connectorId, MaxComputeConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.odps = MaxComputeUtils.getOdps(requireNonNull(config, "config is null"));
        this.supportSchema = MaxComputeUtils.supportSchema(config);
        this.metaCache = new MaxComputeMetaCache(config);
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        if (!supportSchema || schemaName.equals(DEFAULT_SCHEMA)) {
            // non schema model ignore schema
            return true;
        }
        try {
            return odps.schemas().exists(schemaName);
        }
        catch (OdpsException e) {
            throw MaxComputeUtils.wrapOdpsException(e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        if (!supportSchema) {
            return ImmutableList.of(DEFAULT_SCHEMA);
        }
        // TODO: use cache
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (Schema schema : odps.schemas()) {
            builder.add(schema.getName());
        }
        return builder.build();
    }

    @Override
    public MaxComputeTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!schemaExists(session, tableName.getSchemaName())) {
            return null;
        }

        Table table;
        if (supportSchema) {
            table = odps.tables().get(odps.getDefaultProject(), tableName.getSchemaName(), tableName.getTableName());
        }
        else {
            table = odps.tables().get(odps.getDefaultProject(), tableName.getTableName());
        }
        try {
            table.reload();
        }
        catch (NoSuchObjectException e) {
            return null;
        }
        catch (OdpsException e) {
            throw MaxComputeUtils.wrapOdpsException(e);
        }
        MaxComputeTableHandle tableHandle = new MaxComputeTableHandle(odps.getDefaultProject(), tableName.getSchemaName(), tableName.getTableName());
        List<ColumnMetadata> columnMetadata = table.getSchema().getAllColumns().stream().map(column -> new ColumnMetadata(column.getName(), TypeConvertUtils.toPrestoType(column.getTypeInfo()))).collect(Collectors.toList());
        metaCache.writeTableMetadata(tableHandle, new ConnectorTableMetadata(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()), columnMetadata));
        return tableHandle;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        MaxComputeTableHandle maxComputeTableHandle = (MaxComputeTableHandle) table;
        if (desiredColumns.isPresent()) {
            maxComputeTableHandle = maxComputeTableHandle.withProjectedColumns(ImmutableList.copyOf(desiredColumns.get()));
        }
        maxComputeTableHandle = maxComputeTableHandle.withConstraint(constraint.getSummary());
        MaxComputeTableLayoutHandle maxComputeTableLayoutHandle = new MaxComputeTableLayoutHandle(maxComputeTableHandle);
        // TODO: deal with partition
        return ImmutableList.of(new ConnectorTableLayoutResult(getTableLayout(session, maxComputeTableLayoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        MaxComputeTableLayoutHandle maxComputeTableLayoutHandle = (MaxComputeTableLayoutHandle) layoutHandle;
        return new ConnectorTableLayout(maxComputeTableLayoutHandle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        MaxComputeTableHandle maxComputeTableHandle = (MaxComputeTableHandle) table;
        return metaCache.getTableMetadata(maxComputeTableHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        Iterator<Table> iterator;
        if (schemaName.isPresent()) {
            if (DEFAULT_SCHEMA.equals(schemaName.get())) {
                return listTables(session, Optional.empty());
            }
            iterator = odps.tables().iterator(odps.getDefaultProject(), schemaName.get(), null, false);
        }
        else {
            iterator = odps.tables().iterator();
        }
        while (iterator.hasNext()) {
            Table table = iterator.next();
            builder.add(new SchemaTableName(table.getSchemaName() == null ? DEFAULT_SCHEMA : table.getSchemaName(), table.getName()));
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ConnectorTableMetadata tableMetadata = metaCache.getTableMetadata((MaxComputeTableHandle) tableHandle);
        return tableMetadata.getColumns().stream().map(e -> new MaxComputeColumnHandle(e.getName(), e.getType())).collect(Collectors.toMap(MaxComputeColumnHandle::getName, e -> e));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchemaName() == null ? Optional.empty() : Optional.of(prefix.getSchemaName()))) {
            if (prefix.matches(tableName)) {
                ConnectorTableMetadata tableMetadata = getTableMetadata(session, new MaxComputeTableHandle(odps.getDefaultProject(), tableName.getSchemaName(), tableName.getTableName()));
                // table can disappear during listing operation
                if (tableMetadata != null) {
                    columns.put(tableName, tableMetadata.getColumns());
                }
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((MaxComputeColumnHandle) columnHandle).getColumnMetadata();
    }
}
