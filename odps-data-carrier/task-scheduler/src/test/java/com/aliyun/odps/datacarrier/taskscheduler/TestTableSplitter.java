package com.aliyun.odps.datacarrier.taskscheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.Config;

public class TestTableSplitter {
  static private String dataBase = "TestDataBase";
  static private String tableName = "TestTable";
  static private String columnName = "ds";
  static private int date = 20200218;
  private TaskScheduler taskScheduler;
  private MetaConfiguration metaConfiguration;

  @Before
  public void setup() throws IOException {
    taskScheduler = new TaskScheduler();
    taskScheduler.initActions(DataSource.Hive);
    metaConfiguration = MetaConfigurationUtils.generateSampleMetaConfiguration(null, null);
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithNonPartitionedTable() {
    TableSplitter tableSplitter = new TableSplitter(null);

    MetaSource.TableMetaModel tableMetaModel =
        createNonPartitionedTableMetaModel("non_partitioned");
    Config config = createConfig(5);
    Task task = tableSplitter.generateTaskForNonPartitionedTable(tableMetaModel,
                                                                 config,
                                                                 taskScheduler.getActions());

    assertTrue(task.tableMetaModel.partitions.isEmpty());
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable0() {
    TableSplitter tableSplitter = new TableSplitter(null);

    Config config = createConfig(1);
    MetaSource.TableMetaModel tableMetaModel =
        createPartitionedTableMetaModel("partitioned", 5);

    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
                                                                     config,
                                                                     taskScheduler.getActions());

    assertEquals(5, tasks.size());
    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
    assertEquals("20200218",
                 tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200219",
                 tasks.get(1).tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200220",
                 tasks.get(2).tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200221",
                 tasks.get(3).tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200222",
                 tasks.get(4).tableMetaModel.partitions.get(0).partitionValues.get(0));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable1() {
    TableSplitter tableSplitter = new TableSplitter(null);

    Config config = createConfig(10);
    MetaSource.TableMetaModel tableMetaModel =
        createPartitionedTableMetaModel("partitioned", 5);
    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
                                                                     config,
                                                                     taskScheduler.getActions());

    assertEquals(1, tasks.size());
    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
    assertEquals("20200218",
                 tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200219",
                 tasks.get(0).tableMetaModel.partitions.get(1).partitionValues.get(0));
    assertEquals("20200220",
                 tasks.get(0).tableMetaModel.partitions.get(2).partitionValues.get(0));
    assertEquals("20200221",
                 tasks.get(0).tableMetaModel.partitions.get(3).partitionValues.get(0));
    assertEquals("20200222",
                 tasks.get(0).tableMetaModel.partitions.get(4).partitionValues.get(0));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable2() {
    TableSplitter tableSplitter = new TableSplitter(null);

    Config config = createConfig(3);
    MetaSource.TableMetaModel tableMetaModel =
        createPartitionedTableMetaModel("partitioned", 5);
    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
                                                                     config,
                                                                     taskScheduler.getActions());

    assertEquals(2, tasks.size());
    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
    Task task0 = tasks.get(0);
    assertEquals("20200218", task0.tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200219", task0.tableMetaModel.partitions.get(1).partitionValues.get(0));
    assertEquals("20200220", task0.tableMetaModel.partitions.get(2).partitionValues.get(0));

    Task task1 = tasks.get(1);
    assertEquals("20200221", task1.tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200222", task1.tableMetaModel.partitions.get(1).partitionValues.get(0));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable3() {
    TableSplitter tableSplitter = new TableSplitter(null);

    Config config = createConfig(4);
    MetaSource.TableMetaModel tableMetaModel =
        createPartitionedTableMetaModel("partitioned", 7);
    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
                                                                     config,
                                                                     taskScheduler.getActions());

    assertEquals(2, tasks.size());
    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
    Task task0 = tasks.get(0);
    assertEquals("20200218", task0.tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200219", task0.tableMetaModel.partitions.get(1).partitionValues.get(0));
    assertEquals("20200220", task0.tableMetaModel.partitions.get(2).partitionValues.get(0));
    assertEquals("20200221", task0.tableMetaModel.partitions.get(3).partitionValues.get(0));

    Task task1 = tasks.get(1);
    assertEquals("20200222", task1.tableMetaModel.partitions.get(0).partitionValues.get(0));
    assertEquals("20200223", task1.tableMetaModel.partitions.get(1).partitionValues.get(0));
    assertEquals("20200224", task1.tableMetaModel.partitions.get(2).partitionValues.get(0));
  }

  public Config createConfig(int partitionGroupSize) {
    return new Config(null,
                      null,
                      partitionGroupSize,
                      5,
                      "");
  }

  public MetaSource.TableMetaModel createNonPartitionedTableMetaModel(String suffix) {
    MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
    tableMetaModel.databaseName = dataBase;
    tableMetaModel.tableName = tableName + "_" + suffix;

    return tableMetaModel;
  }

  public MetaSource.TableMetaModel createPartitionedTableMetaModel(String suffix,
                                                                   int numOfPartition) {
    MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
    tableMetaModel.databaseName = dataBase;
    tableMetaModel.tableName = tableName + "_" + suffix;

    MetaSource.ColumnMetaModel columnMetaModel = new MetaSource.ColumnMetaModel();
    columnMetaModel.columnName = columnName;
    tableMetaModel.partitionColumns.add(columnMetaModel);
    tableMetaModel.partitions = createPartitions(date, numOfPartition);

    return tableMetaModel;
  }

  public static List<MetaSource.PartitionMetaModel> createPartitions(int date, int numOfPartition) {
    List<MetaSource.PartitionMetaModel> partitions = new ArrayList<>();
    for (int p = 0; p < numOfPartition; p++) {
      MetaSource.PartitionMetaModel partitionMetaModel = new MetaSource.PartitionMetaModel();
      partitionMetaModel.partitionValues.add(String.valueOf(date + p));
      partitions.add(partitionMetaModel);
    }
    return partitions;
  }

}
