package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
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
    for (Table table : odps.tables()) {
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

  /**
   * Get partition values list of given table
   * @param databaseName database name
   * @param tableName table name
   * @return Partition table left partitions need to migrate data.
   * @throws Exception
   */
  @Override
  public List<List<String>> listPartitions(String databaseName,
                                           String tableName) {
    List<List<String>> allPartitiionValues = new ArrayList<>();
    Table table = odps.tables().get(databaseName, tableName);
    for (Partition partition : table.getPartitions()) {
      List<String> partitionValues = new ArrayList<>();
      PartitionSpec partitionSpec = partition.getPartitionSpec();
      for(String key : partitionSpec.keys()) {
        partitionValues.add(partitionSpec.get(key));
      }
      allPartitiionValues.add(partitionValues);
    }
    return allPartitiionValues;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName,
                                             String tableName,
                                             List<String> partitionValues) {
    Table table = odps.tables().get(databaseName, tableName);
    TableMetaModel tableMetaModel = getTableMeta(databaseName, tableName);
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
    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.databaseName = databaseName;
    tableMetaModel.tableName = tableName;
    tableMetaModel.comment = table.getComment();
    tableMetaModel.location = table.getLocation();
    tableMetaModel.size = table.getSize();
    tableMetaModel.serDeProperties = table.getSerDeProperties();

    TableSchema tableSchema = table.getSchema();
    for (Column column : tableSchema.getColumns()) {
      tableMetaModel.columns.add(getColumnMetaModelInternal(column));
    }
    for (Column column : tableSchema.getPartitionColumns()) {
      tableMetaModel.partitionColumns.add(getColumnMetaModelInternal(column));
    }
    if (withPartition) {
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
    PartitionSpec partitionSpec = partition.getPartitionSpec();
    for (String key : partitionSpec.keys()) {
      partitionMetaModel.partitionValues.add(partitionSpec.get(key));
    }
    return partitionMetaModel;
  }

}
