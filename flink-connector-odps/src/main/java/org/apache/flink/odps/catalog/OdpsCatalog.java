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

package org.apache.flink.odps.catalog;

import com.aliyun.odps.Column;
import com.aliyun.odps.*;
import com.aliyun.odps.task.SQLTask;
import com.google.common.cache.LoadingCache;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.table.factories.OdpsDynamicTableFactory;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.odps.table.factories.OdpsDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

public class OdpsCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(OdpsCatalog.class);

    private LoadingCache<String, Optional<Project>> projectCache;
    private LoadingCache<ObjectPath, Optional<Table>> tableCache;

    private static final List<String> allProject = new ArrayList<>();
    private Odps odps;
    private final OdpsConf odpsConf;

    private String getDefaultProject() {
        return odps.getDefaultProject();
    }

    public OdpsCatalog(String name, String defaultDatabase) {
        this(name, defaultDatabase, null);
    }

    public OdpsCatalog(String name, String defaultDatabase, OdpsConf odpsConf) {
        super(name, defaultDatabase);
        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");
    }

    public OdpsConf getOdpsConf() {
        return this.odpsConf;
    }

    private void initMetaCache() {
        OdpsMetaDataProvider odpsMetaDataProvider = new OdpsMetaDataProvider(this.odps);
        this.projectCache = odpsMetaDataProvider.projectCache;
        this.tableCache = odpsMetaDataProvider.tableCache;
    }

    @Override
    public void open() throws CatalogException {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
            this.odps.setDefaultProject(getDefaultDatabase());
            LOG.info("Connected to ODPS metastore");
        }
        initMetaCache();
        if (!databaseExists(getDefaultDatabase())) {
            throw new CatalogException(String.format("Configured default project %s doesn't exist in catalog %s.",
                    getDefaultDatabase(), getName()));
        }
    }

    @Override
    public void close() throws CatalogException {
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new OdpsDynamicTableFactory(this.odpsConf));
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            if (allProject.size() == 0) {
                allProject.add(getDefaultProject());
            }
            return allProject;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in %s", getName()), e);
        }
    }

    private Optional<Project> getProjectOption(String projectName, boolean refresh) throws ExecutionException {
        if (refresh) {
            projectCache.invalidate(projectName);
        }
        return projectCache.get(projectName);
    }

    private Optional<Project> getProjectOption(String projectName) throws ExecutionException {
        return getProjectOption(projectName, false);
    }

    private Optional<Table> getOdpsTableOption(ObjectPath objectPath, boolean refresh) throws ExecutionException {
        if (refresh) {
            tableCache.invalidate(objectPath);
        }
        return tableCache.get(objectPath);
    }

    private Optional<Table> getOdpsTableOption(ObjectPath objectPath) throws ExecutionException {
        return getOdpsTableOption(objectPath, false);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        try {
            Project project = getProjectOption(databaseName, false)
                    .orElseThrow(() -> new DatabaseNotExistException(this.getName(), databaseName));
            return new CatalogDatabaseImpl(project.getProperties(), project.getComment());
        } catch (ExecutionException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return getProjectOption(databaseName, true).isPresent();
        } catch (ExecutionException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists) throws CatalogException {
        throw new CatalogException("create database not supported");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists) throws CatalogException {
        throw new CatalogException("drop database not supported");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) throws CatalogException {
        throw new CatalogException("drop database not supported");
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists) throws CatalogException {
        throw new CatalogException("alter database not supported");
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            Table odpsTable = getOdpsTableOption(tablePath, false)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            return instantiateCatalogTable(odpsTable);
        } catch (ExecutionException e) {
            throw new CatalogException(e);
        }
    }

    public CatalogBaseTable instantiateCatalogTable(Table odpsTable) {
        boolean isView = odpsTable.isVirtualView();
        // TODO: table properties
        Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTOR.key(), IDENTIFIER);
        properties.put(OdpsOptions.TABLE_PATH.key(),
                OdpsTablePath.toTablePath(odpsTable.getProject(), odpsTable.getName()));
        // TODO: support persist properties
        String comment = odpsTable.getComment();

        // Table schema
        List<Column> fields = odpsTable.getSchema().getColumns();
        org.apache.flink.table.api.TableSchema tableSchema =
                OdpsTableUtil.createTableSchema(fields, odpsTable.getSchema().getPartitionColumns());

        // Partition keys
        List<String> partitionKeys = new ArrayList<>();
        if (odpsTable.getSchema().getPartitionColumns().size() > 0) {
            partitionKeys = odpsTable.getSchema()
                    .getPartitionColumns()
                    .stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
        }

        // TODO: view params
        if (isView) {
            throw new CatalogException("OdpsCatalog only supports odps table");
        } else {
            return new CatalogTableImpl(tableSchema, partitionKeys, properties, comment);
        }
    }

    private TableSchema instantiateOdpsTable(CatalogBaseTable table) {
        TableSchema tableSchema = new TableSchema();
        List<Column> allColumns = OdpsTableUtil.createOdpsColumns(table.getSchema());
        // Table columns and partition keys
        if (table instanceof CatalogTable) {
            CatalogTable catalogTable = (CatalogTable) table;
            if (catalogTable.isPartitioned()) {
                int partitionKeySize = catalogTable.getPartitionKeys().size();
                List<Column> regularColumns =
                        allColumns.subList(0, allColumns.size() - partitionKeySize);
                ArrayList<Column> partitionColumns =
                        new ArrayList<>(allColumns.subList(
                                allColumns.size() - partitionKeySize, allColumns.size()));
                tableSchema.setColumns(regularColumns);
                tableSchema.setPartitionColumns(partitionColumns);
            } else {
                tableSchema.setColumns(allColumns);
                tableSchema.setPartitionColumns(new ArrayList<>());
            }
        } else {
            // TODO: CatalogView
            throw new CatalogException(
                    "OdpsCatalog only supports CatalogTable");
        }

        return tableSchema;
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        try {
            return getOdpsTableOption(objectPath, true).isPresent();
        } catch (ExecutionException e) {
            throw new CatalogException(e);
        }
    }

    public static boolean isOdpsTable(Map<String, String> properties) {
        if (properties.containsKey(CONNECTOR.key())) {
            return IDENTIFIER.equalsIgnoreCase(properties.get(CONNECTOR.key()));
        }
        return true;
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkNotNull(table, "table cannot be null");

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        // check connector key
        if (!isOdpsTable(table.getOptions())) {
            throw new CatalogException("Currently odps catalog only supports for odps tables");
        }
        TableSchema tableSchema = instantiateOdpsTable(table);
        try {
            tableCache.invalidate(tablePath);
            Map<String, String> hints = new HashMap<String, String>();
            hints.put("odps.sql.type.system.odps2", "true");
            hints.put("odps.sql.decimal.odps2", "true");
            hints.put("odps.sql.hive.compatible", "false");
            odps.tables().create(tablePath.getDatabaseName(),
                    tablePath.getObjectName(),
                    tableSchema,
                    table.getComment(),
                    ignoreIfExists,
                    null,
                    hints,
                    null);
        } catch (OdpsException e) {
            if (e.getMessage().contains("Table or view already exists"))
                throw new TableAlreadyExistException(getName(), tablePath);
            else {
                throw new CatalogException(String.format("Failed to create table %s", tablePath.getFullName()), e);
            }
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName), "newTableName cannot be null or empty");

        try {
            if (tableExists(tablePath)) {
                ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
                if (tableExists(newPath)) {
                    throw new TableAlreadyExistException(getName(), newPath);
                } else {
                    tableCache.invalidate(tablePath);
                    Table table = getOdpsTableOption(tablePath, true)
                            .orElseThrow(() -> new TableNotExistException(getName(), tablePath));
                    String sql = null;
                    if (table.isVirtualView()) {
                        sql = "ALTER VIEW " + tablePath.getDatabaseName() + "." + tablePath.getObjectName() + " RENAME TO " + newTableName + ";";
                    } else {
                        sql = "ALTER TABLE " + tablePath.getDatabaseName() + "." + tablePath.getObjectName() + " RENAME TO " + newTableName + ";";
                    }
                    SQLTask.run(odps, sql).waitForSuccess();
                    ObjectPath objectPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
                    if (!tableExists(objectPath)) {
                        throw new CatalogException(
                                String.format("Failed to rename table %s", tablePath.getFullName()));
                    }
                }
            } else if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
        } catch (ExecutionException | OdpsException e) {
            throw new CatalogException(
                    String.format("Failed to rename table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newCatalogTable, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("alter table not supported");
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        if (!tableExists(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(this.getName(), tablePath);
        }
        try {
            getOdpsTableOption(tablePath).ifPresent(table -> {
                try {
                    tableCache.invalidate(tablePath);
                    if (table.isVirtualView()) {
                        SQLTask.run(odps, String.format("DROP VIEW %s.%s;)", table.getProject(), table.getName())).waitForSuccess();
                    } else {
                        StringBuilder dropSql = new StringBuilder();
                        dropSql.append("DROP TABLE");
                        if (ignoreIfNotExists) {
                            dropSql.append(" IF EXISTS");
                        }
                        dropSql.append(String.format(" %s.%s;", table.getProject(), table.getName()));
                        SQLTask.run(odps, dropSql.toString()).waitForSuccess();
                    }
                } catch (OdpsException e) {
                    throw new CatalogException("failed to drop table", e);
                }
            });
        } catch (ExecutionException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        Odps odpsClone = odps.clone();
        odpsClone.setDefaultProject(databaseName);
        Iterator<Table> iterator = odpsClone.tables().iterator();
        List<String> tableNames = new ArrayList<>();
        while (iterator.hasNext()) {
            tableNames.add(iterator.next().getName());
        }
        return tableNames;
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        Odps odpsClone = odps.clone();
        odpsClone.setDefaultProject(databaseName);
        Iterator<Table> iterator = odpsClone.tables().iterator();
        List<String> viewNames = new ArrayList<>();
        while (iterator.hasNext()) {
            Table table = iterator.next();
            if (table.isVirtualView()) {
                viewNames.add(table.getName());
            }
        }
        return viewNames;
    }

    private static CatalogPartitionSpec createPartitionSpec(Partition odpsPartition) {
        Map<String, String> spec = new LinkedHashMap<>(odpsPartition.getPartitionSpec().keys().size());
        for (String keyVal : odpsPartition.getPartitionSpec().keys()) {
            spec.put(keyVal, odpsPartition.getPartitionSpec().get(keyVal));
        }
        return new CatalogPartitionSpec(spec);
    }

    private static PartitionSpec createOdpsPartitionSpec(CatalogPartitionSpec catalogPartitionSpec) {

        Map<String, String> spec = catalogPartitionSpec.getPartitionSpec();
        String partition = spec.keySet().stream().map(colName -> colName + "=" + spec.get(colName)).collect(Collectors.joining(","));
        return new PartitionSpec(partition);
    }

    private static CatalogPartitionSpec createPartitionSpecfromPartial(Partition odpsPartition, CatalogPartitionSpec partitionSpec) {
        Map<String, String> spec = new LinkedHashMap<>(odpsPartition.getPartitionSpec().keys().size());

        Iterator<String> partialKeySet = partitionSpec.getPartitionSpec().keySet().iterator();
        Iterator<String> partialOdpsKeySet = odpsPartition.getPartitionSpec().keys().iterator();
        boolean isPartialEqual = true;

        while (partialKeySet.hasNext() && partialOdpsKeySet.hasNext()) {
            String partialKey = partialKeySet.next();
            String odpsKey = partialOdpsKeySet.next();
            String partialValue = partitionSpec.getPartitionSpec().get(partialKey);
            String odpsValue = odpsPartition.getPartitionSpec().get(odpsKey);
            if (partialKey.equals(odpsKey) && partialValue.equals(odpsValue)) {
                spec.put(odpsKey, odpsValue);
            } else {
                isPartialEqual = false;
                break;
            }
        }
        if (!partialKeySet.hasNext() && isPartialEqual) {
            while (partialOdpsKeySet.hasNext()) {
                String odpsKey = partialOdpsKeySet.next();
                String odpsValue = odpsPartition.getPartitionSpec().get(odpsKey);
                spec.put(odpsKey, odpsValue);
            }
            return new CatalogPartitionSpec(spec);
        } else {
            return null;
        }
    }

    private void ensurePartitionedTable(ObjectPath tablePath, Table odpsTable) throws TableNotPartitionedException {
        try {
            if (!odpsTable.isPartitioned()) {
                throw new TableNotPartitionedException(getName(), tablePath);
            }
        } catch (OdpsException e) {
            throw new CatalogException(e);
        }
    }

    private List<String> getOrderedFullPartitionValues(
            CatalogPartitionSpec partitionSpec, List<String> partitionKeys, ObjectPath tablePath)
            throws PartitionSpecInvalidException {
        Map<String, String> spec = partitionSpec.getPartitionSpec();
        if (spec.size() != partitionKeys.size()) {
            throw new PartitionSpecInvalidException(
                    getName(), partitionKeys, tablePath, partitionSpec);
        }

        List<String> values = new ArrayList<>(spec.size());
        for (String key : partitionKeys) {
            if (!spec.containsKey(key)) {
                throw new PartitionSpecInvalidException(
                        getName(), partitionKeys, tablePath, partitionSpec);
            } else {
                values.add(spec.get(key));
            }
        }
        return values;
    }

    private PartitionSpec instantiateOdpsPartition(
            Table odpsTable, CatalogPartitionSpec partitionSpec)
            throws PartitionSpecInvalidException {
        List<String> partCols = odpsTable.getSchema()
                .getPartitionColumns()
                .stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        List<String> partValues =
                getOrderedFullPartitionValues(
                        partitionSpec,
                        partCols,
                        new ObjectPath(odpsTable.getProject(), odpsTable.getName()));
        // validate partition values
        for (int i = 0; i < partCols.size(); i++) {
            if (isNullOrWhitespaceOnly(partValues.get(i))) {
                throw new PartitionSpecInvalidException(
                        getName(),
                        partCols,
                        new ObjectPath(odpsTable.getProject(), odpsTable.getName()),
                        partitionSpec);
            }
        }
        return createOdpsPartitionSpec(partitionSpec);
    }

    @Override
    public void createPartition(ObjectPath tablePath,
                                CatalogPartitionSpec partitionSpec,
                                CatalogPartition partition,
                                boolean ignoreIfExists)
            throws TableNotExistException,
            TableNotPartitionedException,
            PartitionSpecInvalidException,
            CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Table odpsTable = getOdpsTableOption(tablePath)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            ensurePartitionedTable(tablePath, odpsTable);
            odpsTable.createPartition(instantiateOdpsPartition(odpsTable, partitionSpec), ignoreIfExists);
        } catch (ExecutionException | OdpsException e) {
            throw new CatalogException(
                    String.format("Failed to create partition %s of table %s", partitionSpec, tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");

        try {
            Table odpsTable = getOdpsTableOption(tablePath)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            ensurePartitionedTable(tablePath, odpsTable);
            return odpsTable.getPartitions()
                    .stream()
                    .filter(Objects::nonNull)
                    .map(OdpsCatalog::createPartitionSpec)
                    .collect(Collectors.toList());
        } catch (ExecutionException e) {
            throw new CatalogException(
                    String.format("Failed to list partition of table %s", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Table odpsTable = getOdpsTableOption(tablePath)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            ensurePartitionedTable(tablePath, odpsTable);
            // can be partial
            return odpsTable.getPartitions()
                    .stream()
                    .filter(Objects::nonNull)
                    .map(partition -> createPartitionSpecfromPartial(partition, partitionSpec))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (ExecutionException e) {
            throw new CatalogException(
                    String.format("Failed to list partition of table %s", tablePath), e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> expressions)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Partition odpsPartition = getOdpsPartition(tablePath, partitionSpec);
            // TODO: more info
            Map<String, String> properties = new HashMap<>();
            return new CatalogPartitionImpl(properties, "comment");
        } catch (TableNotExistException e) {
            throw new CatalogException(
                    String.format("Failed to get partition %s of table %s", partitionSpec, tablePath), e);
        }
    }

    private Partition getOdpsPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws TableNotExistException {
        try {
            Table odpsTable = getOdpsTableOption(tablePath).orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            ensurePartitionedTable(tablePath, odpsTable);
            Partition partition = odpsTable.getPartition(createOdpsPartitionSpec(partitionSpec));
            partition.reload();
            return partition;
        } catch (ExecutionException | TableNotPartitionedException | OdpsException e) {
            throw new CatalogException(
                    String.format("Failed to list partition of table %s", tablePath), e);
        }
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Table odpsTable = getOdpsTableOption(tablePath)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            ensurePartitionedTable(tablePath, odpsTable);
            return odpsTable.hasPartition(createOdpsPartitionSpec(partitionSpec));
        } catch (TableNotExistException | OdpsException | TableNotPartitionedException | ExecutionException e) {
            throw new CatalogException(
                    String.format("Failed to get partition %s of table %s", partitionSpec, tablePath), e);
        }
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(partitionSpec, "CatalogPartitionSpec cannot be null");

        try {
            Table odpsTable = getOdpsTableOption(tablePath)
                    .orElseThrow(() -> new TableNotExistException(this.getName(), tablePath));
            odpsTable.deletePartition(createOdpsPartitionSpec(partitionSpec), ignoreIfNotExists);
        } catch (OdpsException | TableNotExistException | ExecutionException e) {
            throw new CatalogException(
                    String.format("Failed to drop partition %s of table %s", partitionSpec, tablePath), e);
        }
    }

    @Override
    public void alterPartition(ObjectPath tablePath,
                               CatalogPartitionSpec partitionSpec,
                               CatalogPartition newPartition,
                               boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listFunctions(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        Table table = odps.tables().get(tablePath.getDatabaseName(), tablePath.getObjectName());
        return new CatalogTableStatistics(table.getSize(),
                (int) table.getFileNum(), table.getPhysicalSize(), table.getSize());
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        try {
            Partition partition = getOdpsPartition(tablePath, partitionSpec);
            return new CatalogTableStatistics(partition.getSize(),
                    (int) partition.getFileNum(), partition.getPhysicalSize(), partition.getSize());
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get partition stats of table %s 's partition %s",
                            tablePath.getFullName(), partitionSpec),
                    e);
        }
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath,
                                         CatalogPartitionSpec partitionSpec,
                                         CatalogTableStatistics partitionStatistics,
                                         boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath,
                                               CatalogPartitionSpec partitionSpec,
                                               CatalogColumnStatistics columnStatistics,
                                               boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
