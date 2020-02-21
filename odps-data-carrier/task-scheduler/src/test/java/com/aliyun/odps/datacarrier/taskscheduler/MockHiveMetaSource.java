package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource;

public class MockHiveMetaSource implements MetaSource {

  private static final String TABLE_NAME_NON_PARTITIONED = "test_non_partitioned";
  private static final String TABLE_NAME_PARTITIONED = "test_partitioned";
  private static final String DB_NAME = "test";


  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    if (!DB_NAME.equals(databaseName)) {
      throw new IllegalArgumentException("no such db");
    }

    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.tableName = tableName;
    tableMetaModel.databaseName = databaseName;

    ColumnMetaModel c = new ColumnMetaModel();
    c.columnName = "foo";
    c.type = "string";
    tableMetaModel.columns.add(c);

    if (TABLE_NAME_PARTITIONED.equals(tableName)) {
      ColumnMetaModel pc = new ColumnMetaModel();
      pc.columnName = "bar";
      pc.type = "string";
      tableMetaModel.partitionColumns.add(pc);

      PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
      partitionMetaModel.partitionValues.add("hello_world");
      tableMetaModel.partitions.add(partitionMetaModel);
    }

    return tableMetaModel;
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName, String tableName)
      throws Exception {
    TableMetaModel tableMetaModel = getTableMeta(databaseName, tableName);
    tableMetaModel.partitions.clear();
    return tableMetaModel;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName, String tableName,
                                             List<String> partitionValues) throws Exception {
    if (!DB_NAME.equals(databaseName) ||
        !TABLE_NAME_PARTITIONED.equals(tableName) ||
        partitionValues.size() != 1 ||
        !"hello_world".equals(partitionValues.get(0))) {
      throw new IllegalArgumentException("invalid args");
    }

    PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
    partitionMetaModel.partitionValues.add("hello_world");
    return partitionMetaModel;
  }

  @Override
  public List<String> listTables(String databaseName) throws Exception {
    List<String> ret = new ArrayList<>();

    if (DB_NAME.equals(databaseName)) {
      ret.add(TABLE_NAME_NON_PARTITIONED);
      ret.add(TABLE_NAME_PARTITIONED);
    }
    return ret;
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
  public List<String> listDatabases() throws Exception {
    List<String> ret = new LinkedList<>();
    ret.add(DB_NAME);
    return ret;
  }
}
