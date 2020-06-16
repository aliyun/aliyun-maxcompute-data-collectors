///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.aliyun.odps.datacarrier.taskscheduler;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import org.junit.Before;
//import org.junit.Test;
//
//
//public class TestTableSplitter {
//  static private String dataBase = "TestDataBase";
//  static private String tableName = "TestTable";
//  static private String columnName = "ds";
//  static private int date = 20200218;
//  private TaskScheduler taskScheduler;
//
//  @Before
//  public void setup() throws IOException, MmaException {
//    taskScheduler = new TaskScheduler();
//    taskScheduler.initActions(DataSource.Hive);
//  }
//
//  @Test(timeout = 5000)
//  public void testGenerateTasksWithNonPartitionedTable() {
//    TableSplitter tableSplitter = new TableSplitter(null, null);
//
//    MetaSource.TableMetaModel tableMetaModel =
//        createNonPartitionedTableMetaModel("non_partitioned");
//    MmaConfig.AdditionalTableConfig additionalTableConfig =
//        createAdditionalTableConfig(5);
//    Task task = tableSplitter.generateTaskForNonPartitionedTable(tableMetaModel,
//                                                                 additionalTableConfig,
//                                                                 taskScheduler.getActions());
//
//    assertTrue(task.tableMetaModel.partitions.isEmpty());
//  }
//
//  @Test(timeout = 5000)
//  public void testGenerateTasksWithPartitionedTable0() {
//    TableSplitter tableSplitter = new TableSplitter(null, null);
//
//    MmaConfig.AdditionalTableConfig additionalTableConfig =
//        createAdditionalTableConfig(1);
//    MetaSource.TableMetaModel tableMetaModel =
//        createPartitionedTableMetaModel("partitioned", 5);
//
//    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
//                                                                     additionalTableConfig,
//                                                                     taskScheduler.getActions());
//
//    assertEquals(5, tasks.size());
//    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
//    assertEquals("20200218",
//                 tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200219",
//                 tasks.get(1).tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200220",
//                 tasks.get(2).tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200221",
//                 tasks.get(3).tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200222",
//                 tasks.get(4).tableMetaModel.partitions.get(0).partitionValues.get(0));
//  }
//
//  @Test(timeout = 5000)
//  public void testGenerateTasksWithPartitionedTable1() {
//    TableSplitter tableSplitter = new TableSplitter(null, null);
//
//    MmaConfig.AdditionalTableConfig additionalTableConfig = createAdditionalTableConfig(10);
//    MetaSource.TableMetaModel tableMetaModel =
//        createPartitionedTableMetaModel("partitioned", 5);
//    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
//                                                                     additionalTableConfig,
//                                                                     taskScheduler.getActions());
//
//    assertEquals(1, tasks.size());
//    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
//    assertEquals("20200218",
//                 tasks.get(0).tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200219",
//                 tasks.get(0).tableMetaModel.partitions.get(1).partitionValues.get(0));
//    assertEquals("20200220",
//                 tasks.get(0).tableMetaModel.partitions.get(2).partitionValues.get(0));
//    assertEquals("20200221",
//                 tasks.get(0).tableMetaModel.partitions.get(3).partitionValues.get(0));
//    assertEquals("20200222",
//                 tasks.get(0).tableMetaModel.partitions.get(4).partitionValues.get(0));
//  }
//
//  @Test(timeout = 5000)
//  public void testGenerateTasksWithPartitionedTable2() {
//    TableSplitter tableSplitter = new TableSplitter(null, null);
//
//    MmaConfig.AdditionalTableConfig additionalTableConfig = createAdditionalTableConfig(3);
//    MetaSource.TableMetaModel tableMetaModel =
//        createPartitionedTableMetaModel("partitioned", 5);
//    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
//                                                                     additionalTableConfig,
//                                                                     taskScheduler.getActions());
//
//    assertEquals(2, tasks.size());
//    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
//    Task task0 = tasks.get(0);
//    assertEquals("20200218", task0.tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200219", task0.tableMetaModel.partitions.get(1).partitionValues.get(0));
//    assertEquals("20200220", task0.tableMetaModel.partitions.get(2).partitionValues.get(0));
//
//    Task task1 = tasks.get(1);
//    assertEquals("20200221", task1.tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200222", task1.tableMetaModel.partitions.get(1).partitionValues.get(0));
//  }
//
//  @Test(timeout = 5000)
//  public void testGenerateTasksWithPartitionedTable3() {
//    TableSplitter tableSplitter = new TableSplitter(null, null);
//
//    MmaConfig.AdditionalTableConfig additionalTableConfig = createAdditionalTableConfig(4);
//    MetaSource.TableMetaModel tableMetaModel =
//        createPartitionedTableMetaModel("partitioned", 7);
//    List<Task> tasks = tableSplitter.generateTaskForPartitionedTable(tableMetaModel,
//                                                                     additionalTableConfig,
//                                                                     taskScheduler.getActions());
//
//    assertEquals(2, tasks.size());
//    assertTrue(tasks.stream().noneMatch(t -> t.tableMetaModel.partitions.isEmpty()));
//    Task task0 = tasks.get(0);
//    assertEquals("20200218", task0.tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200219", task0.tableMetaModel.partitions.get(1).partitionValues.get(0));
//    assertEquals("20200220", task0.tableMetaModel.partitions.get(2).partitionValues.get(0));
//    assertEquals("20200221", task0.tableMetaModel.partitions.get(3).partitionValues.get(0));
//
//    Task task1 = tasks.get(1);
//    assertEquals("20200222", task1.tableMetaModel.partitions.get(0).partitionValues.get(0));
//    assertEquals("20200223", task1.tableMetaModel.partitions.get(1).partitionValues.get(0));
//    assertEquals("20200224", task1.tableMetaModel.partitions.get(2).partitionValues.get(0));
//  }
//
//  public MmaConfig.AdditionalTableConfig createAdditionalTableConfig(int partitionGroupSize) {
//    return new MmaConfig.AdditionalTableConfig(null,
//                                               null,
//                                               partitionGroupSize,
//                                               5);
//  }
//
//  public MetaSource.TableMetaModel createNonPartitionedTableMetaModel(String suffix) {
//    MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
//    tableMetaModel.databaseName = dataBase;
//    tableMetaModel.tableName = tableName + "_" + suffix;
//
//    return tableMetaModel;
//  }
//
//  public MetaSource.TableMetaModel createPartitionedTableMetaModel(String suffix,
//                                                                   int numOfPartition) {
//    MetaSource.TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
//    tableMetaModel.databaseName = dataBase;
//    tableMetaModel.tableName = tableName + "_" + suffix;
//
//    MetaSource.ColumnMetaModel columnMetaModel = new MetaSource.ColumnMetaModel();
//    columnMetaModel.columnName = columnName;
//    tableMetaModel.partitionColumns.add(columnMetaModel);
//    tableMetaModel.partitions = createPartitions(date, numOfPartition);
//
//    return tableMetaModel;
//  }
//
//  public static List<MetaSource.PartitionMetaModel> createPartitions(int date, int numOfPartition) {
//    List<MetaSource.PartitionMetaModel> partitions = new ArrayList<>();
//    for (int p = 0; p < numOfPartition; p++) {
//      MetaSource.PartitionMetaModel partitionMetaModel = new MetaSource.PartitionMetaModel();
//      partitionMetaModel.partitionValues.add(String.valueOf(date + p));
//      partitions.add(partitionMetaModel);
//    }
//    return partitions;
//  }
//
//}
