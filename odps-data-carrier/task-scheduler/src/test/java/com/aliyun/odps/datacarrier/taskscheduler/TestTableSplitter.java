package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTableSplitter {
  static private String dataBase = "TestDataBase";
  static private String tableName = "TestTable";
  static private String columnName = "ds";
  static private int date = 20200218;
  private TaskScheduler taskScheduler;
  private MetaConfiguration metaConfiguration;

  @Before
  public void setup() {
    taskScheduler = new TaskScheduler();
    taskScheduler.initActions(DataSource.Hive);
    metaConfiguration = MetaConfigurationUtils.generateSampleMetaConfiguration(null);
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithNonPartitionedTable() {
    TableSplitter tableSplitter = new TableSplitter(updateMetaConfigAndCreateTables(2, 0, 4), metaConfiguration);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t -> t.tableMetaModel.partitions.isEmpty()));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable0() {
    TableSplitter tableSplitter = new TableSplitter(updateMetaConfigAndCreateTables(1, 5, 1), metaConfiguration);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 5);
    assertTrue(tasks.stream().allMatch(t -> !t.tableMetaModel.partitions.isEmpty()));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200218"));
    assertTrue(tasks.get(1).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200219"));
    assertTrue(tasks.get(2).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200220"));
    assertTrue(tasks.get(3).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200221"));
    assertTrue(tasks.get(4).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable1() {
    TableSplitter tableSplitter = new TableSplitter(updateMetaConfigAndCreateTables(1, 5, 10), metaConfiguration);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 1);
    assertTrue(tasks.stream().allMatch(t -> !t.tableMetaModel.partitions.isEmpty()));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200218"));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(1).partitionValues.get(0).equals("20200219"));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(2).partitionValues.get(0).equals("20200220"));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(3).partitionValues.get(0).equals("20200221"));
    assertTrue(tasks.get(0).tableMetaModel.partitions.get(4).partitionValues.get(0).equals("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable2() {
    TableSplitter tableSplitter = new TableSplitter(updateMetaConfigAndCreateTables(1, 5, 3), metaConfiguration);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t -> !t.tableMetaModel.partitions.isEmpty()));
    Task task0 = tasks.get(0);
    assertTrue(task0.tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200218"));
    assertTrue(task0.tableMetaModel.partitions.get(1).partitionValues.get(0).equals("20200219"));
    assertTrue(task0.tableMetaModel.partitions.get(2).partitionValues.get(0).equals("20200220"));

    Task task1 = tasks.get(1);
    assertTrue(task1.tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200221"));
    assertTrue(task1.tableMetaModel.partitions.get(1).partitionValues.get(0).equals("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable3() {
    TableSplitter tableSplitter = new TableSplitter(updateMetaConfigAndCreateTables(1, 7, 4), metaConfiguration);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t -> !t.tableMetaModel.partitions.isEmpty()));
    Task task0 = tasks.get(0);
    assertTrue(task0.tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200218"));
    assertTrue(task0.tableMetaModel.partitions.get(1).partitionValues.get(0).equals("20200219"));
    assertTrue(task0.tableMetaModel.partitions.get(2).partitionValues.get(0).equals("20200220"));
    assertTrue(task0.tableMetaModel.partitions.get(3).partitionValues.get(0).equals("20200221"));

    Task task1 = tasks.get(1);
    assertTrue(task1.tableMetaModel.partitions.get(0).partitionValues.get(0).equals("20200222"));
    assertTrue(task1.tableMetaModel.partitions.get(1).partitionValues.get(0).equals("20200223"));
    assertTrue(task1.tableMetaModel.partitions.get(2).partitionValues.get(0).equals("20200224"));
  }

  public List<MetaSource.TableMetaModel> updateMetaConfigAndCreateTables(int tableNum, int partitionsNum, int numOfPartitions) {
    List<MetaSource.TableMetaModel> tableMetaModels = new ArrayList<>();
    List<TableGroup> tablesGroupList = new ArrayList<>();
    TableGroup tablesGroup = new TableGroup();
    List<TableConfig> tableConfigs = new ArrayList<>();

    for (int i = 0; i < tableNum; i++) {
      String tName = tableName + "_" + i;
      Config config = new Config(null, null, numOfPartitions, 5, "");
      TableConfig table = new TableConfig(dataBase, tName, dataBase, tName, config);
      tableConfigs.add(table);
      MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
      tableMetaModel.databaseName = dataBase;
      tableMetaModel.tableName = tName;
      //Partitioned table.
      if (partitionsNum > 0) {
        tableMetaModel.partitions = createPartitions(date, partitionsNum);
      }
      tableMetaModels.add(tableMetaModel);
    }
    tablesGroup.setTables(tableConfigs);
    tablesGroupList.add(tablesGroup);
    metaConfiguration.setTableGroups(tablesGroupList);
    metaConfiguration.validateAndInitConfig();
    return tableMetaModels;
  }

  public static List<MetaSource.PartitionMetaModel> createPartitions(int date, int partitionsNum) {
    List<MetaSource.PartitionMetaModel> partitions = new ArrayList<>();
    for (int p = 0; p < partitionsNum; p++) {
      MetaSource.PartitionMetaModel partitionMetaModel = new MetaSource.PartitionMetaModel();
      partitionMetaModel.partitionValues.add(String.valueOf(date + p));
      partitions.add(partitionMetaModel);
    }
    return partitions;
  }

}
