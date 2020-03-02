package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class MockHiveMetaSource implements MetaSource {

  private static final String DB_NAME = "test";
  private static final String[] TABLE_NAMES = new String[] {"test_non_partitioned",
                                                            "test_partitioned"};
  private static final Map<String, TableMetaModel> TABLE_NAME_2_TABLE_META_MODEL = new HashMap<>();
  static {
    TABLE_NAME_2_TABLE_META_MODEL.put("test_non_partitioned",
                                      getTestNonPartitionedTableMetaModel());
    TABLE_NAME_2_TABLE_META_MODEL.put("test_partitioned", getTestPartitionedTableMetaModel());
  }

  private static TableMetaModel getTestNonPartitionedTableMetaModel() {
    TableMetaModel testNonPartitioned = new TableMetaModel();
    testNonPartitioned.databaseName = DB_NAME;
    testNonPartitioned.odpsProjectName = DB_NAME;
    testNonPartitioned.tableName = "test_non_partitioned";
    testNonPartitioned.odpsTableName = "test_non_partitioned";

    ColumnMetaModel c = new ColumnMetaModel();
    c.columnName = "foo";
    c.odpsColumnName = "foo";
    c.type = "string";
    c.odpsType = "string";
    testNonPartitioned.columns.add(c);

    return testNonPartitioned;
  }

  private static TableMetaModel getTestPartitionedTableMetaModel() {
    TableMetaModel testPartitioned = new TableMetaModel();
    testPartitioned.databaseName = DB_NAME;
    testPartitioned.odpsProjectName = DB_NAME;
    testPartitioned.tableName = "test_partitioned";
    testPartitioned.odpsTableName = "test_partitioned";

    ColumnMetaModel c = new ColumnMetaModel();
    c.columnName = "foo";
    c.odpsColumnName = "foo";
    c.type = "string";
    c.odpsType = "string";
    testPartitioned.columns.add(c);

    ColumnMetaModel pc = new ColumnMetaModel();
    pc.columnName = "bar";
    pc.odpsColumnName = "bar";
    pc.type = "string";
    pc.odpsType = "string";
    testPartitioned.partitionColumns.add(pc);

    PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
    partitionMetaModel.partitionValues.add("hello_world");
    testPartitioned.partitions.add(partitionMetaModel);

    return testPartitioned;
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    if (!DB_NAME.equals(databaseName)) {
      throw new IllegalArgumentException("database doesn't exist");
    }

    if (!TABLE_NAME_2_TABLE_META_MODEL.containsKey(tableName)) {
      throw new IllegalArgumentException("table doesn't exist");
    }

    return TABLE_NAME_2_TABLE_META_MODEL.get(tableName);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    if (!DB_NAME.equals(databaseName)) {
      throw new IllegalArgumentException("database doesn't exist");
    }

    if (!TABLE_NAME_2_TABLE_META_MODEL.containsKey(tableName)) {
      throw new IllegalArgumentException("table doesn't exist");
    }

    TableMetaModel tableMetaModel;
    if ("test_non_partitioned".equals(tableName)) {
      tableMetaModel = getTestNonPartitionedTableMetaModel();
    } else {
      tableMetaModel = getTestPartitionedTableMetaModel();
    }

    tableMetaModel.partitions.clear();
    return tableMetaModel;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName, String tableName,
                                             List<String> partitionValues) throws Exception {
    if (!DB_NAME.equals(databaseName)) {
     throw new IllegalArgumentException("database doesn't exist");
    }

    if (!TABLE_NAME_2_TABLE_META_MODEL.containsKey(tableName)) {
      throw new IllegalArgumentException("table doesn't exist");
    }

    TableMetaModel tableMetaModel = TABLE_NAME_2_TABLE_META_MODEL.get(tableName);
    for (PartitionMetaModel partitionMetaModel : tableMetaModel.partitions) {
      if (partitionValues.equals(partitionMetaModel.partitionValues)) {
        return partitionMetaModel;
      }
    }

    throw new IllegalArgumentException("partition doesn't exist");
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    List<List<String>> ret = new LinkedList<>();

    for (PartitionMetaModel pt : getTableMeta(databaseName, tableName).partitions) {
      ret.add(pt.partitionValues);
    }
    return ret;
  }

  @Override
  public List<String> listTables(String databaseName) throws Exception {
    return Arrays.asList(TABLE_NAMES);
  }

  @Override
  public List<String> listDatabases() throws Exception {
    List<String> ret = new LinkedList<>();
    ret.add(DB_NAME);
    return ret;
  }
}
