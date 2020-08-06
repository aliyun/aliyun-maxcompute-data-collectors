package com.aliyun.odps.datacarrier.taskscheduler.meta;

import com.aliyun.odps.Column;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.type.TypeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OdpsMetaSource implements MetaSource {

  private static final Logger LOG = LogManager.getLogger(OdpsMetaSource.class);

  private Odps odps;

  public OdpsMetaSource(String accessId,
                        String accessKey,
                        String endpoint,
                        String defaultProject) {
    Account account = new AliyunAccount(accessId, accessKey);
    odps = new Odps(account);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(defaultProject);
  }


  @Override
  public boolean hasDatabase(String databaseName) throws Exception {
    LOG.info("Enter hasDatabase, database name: {}", databaseName);
    return odps.projects().exists(databaseName);
  }

  @Override
  public List<String> listDatabases(){
    List<String> databases = new ArrayList<>();
    Iterator<Project> iterator = odps.projects().iterator(null);
    while (iterator.hasNext()){
      databases.add(iterator.next().getName());
    }
    return databases;
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) throws Exception {
    return odps.tables().exists(databaseName, tableName);
  }

  @Override
  public List<String> listTables(String databaseName) {
    List<String> tables = new ArrayList<>();
    Iterator<Table> iterator = odps.tables().iterator(databaseName);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      if (table.isVirtualView()) {
        continue;
      }
      tables.add(table.getName());
    }
    return tables;
  }

  public List<String> listViews(String databaseName) {
    List<String> views = new ArrayList<>();
    Iterator<Table> iterator = odps.tables().iterator(databaseName);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      if (table.isVirtualView()) {
        views.add(table.getName());
      }
    }
    return views;
  }


  public List<String> listFunctions(String databaseName) {
    List<String> functions = new ArrayList<>();
    Iterator<Function> iterator = odps.functions().iterator(databaseName);
    while (iterator.hasNext()) {
      Function func = iterator.next();
      functions.add(func.getName());
    }
    return functions;
  }

  public List<String> listResources(String databaseName) {
    List<String> resources = new ArrayList<>();
    Iterator<Resource> iterator = odps.resources().iterator(databaseName);
    while (iterator.hasNext()) {
      Resource resource = iterator.next();
      resources.add(resource.getName());
    }
    return resources;
  }

  public List<String> listManagedTables(String databaseName) {
    List<String> tables = new ArrayList<>();
    Iterator<Table> iterator = odps.tables().iterator(databaseName);
    while (iterator.hasNext()) {
      Table table = iterator.next();
      if (table.isExternalTable() || table.isVirtualView()) {
        continue;
      }
      tables.add(table.getName());
    }
    return tables;
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) {
    return getTableMetaInternal(databaseName, tableName ,true);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                         String tableName) {
    return getTableMetaInternal(databaseName, tableName ,false);
  }

  @Override
  public boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws OdpsException {
    if (!odps.tables().exists(databaseName, tableName)) {
      return false;
    }

    List<Column> partitionColumns =
        odps.tables().get(databaseName, tableName).getSchema().getPartitionColumns();
    if (partitionValues.size() != partitionColumns.size()) {
      return false;
    }

    PartitionSpec partitionSpec = new PartitionSpec();
    for (int i = 0; i < partitionValues.size(); i++) {
      partitionSpec.set(partitionColumns.get(i).getName(), partitionValues.get(i));
    }

    return odps.tables().get(databaseName, tableName).hasPartition(partitionSpec);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) {
    List<List<String>> allPartitionValues = new ArrayList<>();
    Table table = odps.tables().get(databaseName, tableName);
    for (Partition partition : table.getPartitions()) {
      List<String> partitionValues = new ArrayList<>();
      PartitionSpec partitionSpec = partition.getPartitionSpec();
      for(String key : partitionSpec.keys()) {
        partitionValues.add(partitionSpec.get(key));
      }
      allPartitionValues.add(partitionValues);
    }
    return allPartitionValues;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName,
                                             String tableName,
                                             List<String> partitionValues) {
    Table table = odps.tables().get(databaseName, tableName);
    TableMetaModel tableMetaModel = getTableMetaInternal(table, true);
    List<ColumnMetaModel> partitionColumns = tableMetaModel.partitionColumns;
    PartitionSpec partitionSpec = new PartitionSpec();
    for (int partIndex = 0; partIndex < partitionValues.size(); partIndex++) {
      partitionSpec.set(partitionColumns.get(partIndex).columnName, partitionValues.get(partIndex));
    }
    Partition partition = table.getPartition(partitionSpec);
    return getPartitionMetaModelInternal(partition);
  }

  @Override
  public void shutdown() {
    odps = null;
  }

  private TableMetaModel getTableMetaInternal(String databaseName,
                                              String tableName,
                                              boolean withPartition) {
    Table table = odps.tables().get(databaseName, tableName);
    return getTableMetaInternal(table, withPartition);
  }

  private TableMetaModel getTableMetaInternal(Table table, boolean withPartition) {
    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.databaseName = table.getProject();
    tableMetaModel.tableName = table.getName();
    tableMetaModel.comment = table.getComment();
    tableMetaModel.location = table.getLocation();
    tableMetaModel.size = table.getSize();
    tableMetaModel.createTime = table.getCreatedTime().getTime() / 1000;
    tableMetaModel.lastModifiedTime = table.getLastDataModifiedTime().getTime() / 1000;

    if (table.getSerDeProperties() != null) {
      tableMetaModel.serDeProperties.putAll(table.getSerDeProperties());
    }

    TableSchema tableSchema = table.getSchema();
    for (Column column : tableSchema.getColumns()) {
      tableMetaModel.columns.add(getColumnMetaModelInternal(column));
    }
    for (Column column : tableSchema.getPartitionColumns()) {
      tableMetaModel.partitionColumns.add(getColumnMetaModelInternal(column));
    }
    if (withPartition && tableMetaModel.partitionColumns.size() > 0) {
      for (Partition partition : table.getPartitions()) {
        tableMetaModel.partitions.add(getPartitionMetaModelInternal(partition));
      }
    }
    return tableMetaModel;
  }

  private ColumnMetaModel getColumnMetaModelInternal(Column column) {
    ColumnMetaModel columnMetaModel = new ColumnMetaModel();
    columnMetaModel.columnName = column.getName();
    TypeInfo columnTypeInfo = column.getTypeInfo();
    columnMetaModel.type = columnTypeInfo.getTypeName();
    columnMetaModel.odpsType = columnTypeInfo.getOdpsType().name();
    columnMetaModel.comment = column.getComment();
    return columnMetaModel;
  }

  private PartitionMetaModel getPartitionMetaModelInternal(Partition partition) {
    PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
    partitionMetaModel.createTime = partition.getCreatedTime().getTime() / 1000;
    partitionMetaModel.lastModifiedTime = partition.getLastDataModifiedTime().getTime() / 1000;
    PartitionSpec partitionSpec = partition.getPartitionSpec();
    for (String key : partitionSpec.keys()) {
      partitionMetaModel.partitionValues.add(partitionSpec.get(key));
    }
    return partitionMetaModel;
  }

}
