package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
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

  @Before
  public void setup() {
    taskScheduler = new TaskScheduler();
    taskScheduler.initActions(DataSource.Hive);
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithNonPartitionedTable() {
    TableSplitter tableSplitter = new TableSplitter(createTables(2, 0), 4);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t->!t.isPartitionTable && t.partitions.isEmpty()));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable0() {
    TableSplitter tableSplitter = new TableSplitter(createTables(1, 5), 1);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 5);
    assertTrue(tasks.stream().allMatch(t->t.isPartitionTable));
    assertTrue(tasks.get(0).partitions.get(0).partitionSpec.containsValue("20200218"));
    assertTrue(tasks.get(1).partitions.get(0).partitionSpec.containsValue("20200219"));
    assertTrue(tasks.get(2).partitions.get(0).partitionSpec.containsValue("20200220"));
    assertTrue(tasks.get(3).partitions.get(0).partitionSpec.containsValue("20200221"));
    assertTrue(tasks.get(4).partitions.get(0).partitionSpec.containsValue("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable1() {
    TableSplitter tableSplitter = new TableSplitter(createTables(1, 5), 10);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 1);
    assertTrue(tasks.stream().allMatch(t->t.isPartitionTable));
    assertTrue(tasks.get(0).partitions.get(0).partitionSpec.containsValue("20200218"));
    assertTrue(tasks.get(0).partitions.get(1).partitionSpec.containsValue("20200219"));
    assertTrue(tasks.get(0).partitions.get(2).partitionSpec.containsValue("20200220"));
    assertTrue(tasks.get(0).partitions.get(3).partitionSpec.containsValue("20200221"));
    assertTrue(tasks.get(0).partitions.get(4).partitionSpec.containsValue("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable2() {
    TableSplitter tableSplitter = new TableSplitter(createTables(1, 5), 3);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t->t.isPartitionTable));
    Task task0 = tasks.get(0);
    assertTrue(task0.partitions.get(0).partitionSpec.containsValue("20200218"));
    assertTrue(task0.partitions.get(1).partitionSpec.containsValue("20200219"));
    assertTrue(task0.partitions.get(2).partitionSpec.containsValue("20200220"));

    Task task1 = tasks.get(1);
    assertTrue(task1.partitions.get(0).partitionSpec.containsValue("20200221"));
    assertTrue(task1.partitions.get(1).partitionSpec.containsValue("20200222"));
  }

  @Test(timeout = 5000)
  public void testGenerateTasksWithPartitionedTable3() {
    TableSplitter tableSplitter = new TableSplitter(createTables(1, 7), 4);
    List<Task> tasks = tableSplitter.generateTasks(taskScheduler.getActions(), Mode.BATCH);

    assertEquals(tasks.size(), 2);
    assertTrue(tasks.stream().allMatch(t->t.isPartitionTable));
    Task task0 = tasks.get(0);
    assertTrue(task0.partitions.get(0).partitionSpec.containsValue("20200218"));
    assertTrue(task0.partitions.get(1).partitionSpec.containsValue("20200219"));
    assertTrue(task0.partitions.get(2).partitionSpec.containsValue("20200220"));
    assertTrue(task0.partitions.get(3).partitionSpec.containsValue("20200221"));

    Task task1 = tasks.get(1);
    assertTrue(task1.partitions.get(0).partitionSpec.containsValue("20200222"));
    assertTrue(task1.partitions.get(1).partitionSpec.containsValue("20200223"));
    assertTrue(task1.partitions.get(2).partitionSpec.containsValue("20200224"));
  }


  static List<TableMetaModel> createTables(int tablesNum, int partitionsNum) {
    List<TableMetaModel> tables = new ArrayList<>();
    for (int i = 0; i < tablesNum; i++) {
      TableMetaModel tableMetaModel = new TableMetaModel();
      tableMetaModel.databaseName = dataBase;
      tableMetaModel.tableName = tableName + "_" + i;
      //Partitioned table.
      if (partitionsNum > 0) {
        tableMetaModel.partitions = createPartitions(partitionsNum);
      }
      tables.add(tableMetaModel);
    }
    return tables;
  }

  static List<PartitionMetaModel> createPartitions(int partitionsNum) {
    List<PartitionMetaModel> partitions = new ArrayList<>();
    for (int p = 0; p < partitionsNum; p++) {
      PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
      partitionMetaModel.partitionSpec.putIfAbsent(columnName, String.valueOf(date + p));
      partitions.add(partitionMetaModel);
    }
    return partitions;
  }

}
