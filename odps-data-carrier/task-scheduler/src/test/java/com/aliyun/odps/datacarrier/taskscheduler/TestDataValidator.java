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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//package com.aliyun.odps.datacarrier.taskscheduler;
//
//import org.junit.Test;
//import java.util.HashMap;
//import java.util.Map;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertTrue;
//
//public class TestDataValidator {
//
//  @Test(timeout = 5000)
//  public void testValidateTaskCountResultByPartition() {
//
//    Task task = new Task("TestDataBase", "TestTable", new MetaSource.TableMetaModel(), null);
//    task.addActionInfo(Action.HIVE_VERIFICATION, task.getTableNameWithProject());
//    task.addActionInfo(Action.ODPS_VERIFICATION, task.getTableNameWithProject());
//    int date = 20200218;
//    int partitionNum = 5;
//    int succeededPartitionNum = 2;
//    task.tableMetaModel.partitions.addAll(TestTableSplitter.createPartitions(date, partitionNum));
//
//    Map<String, String> multiRecordResult = new HashMap<>();
//    for (int i = 0; i < succeededPartitionNum; i++) {
//      multiRecordResult.put(String.valueOf(date + i), String.valueOf(i));
//    }
//
//    multiRecordResult.put("20200226", "12");
//    task.actionInfoMap.get(Action.HIVE_VERIFICATION).executionInfoMap.get(
//        task.getTableNameWithProject()).setMultiRecordResult(multiRecordResult);
//    task.actionInfoMap.get(Action.ODPS_VERIFICATION).executionInfoMap.get(
//        task.getTableNameWithProject()).setMultiRecordResult(multiRecordResult);
//
//    DataValidator dataValidator = new DataValidator();
//    DataValidator.ValidationResult result = dataValidator.validationPartitions(task);
//    assertTrue(result.succeededPartitions.size() == 2);
//    assertEquals(result.succeededPartitions.get(0).get(0), "20200218");
//    assertEquals(result.succeededPartitions.get(1).get(0), "20200219");
//    assertTrue(result.failedPartitions.size() == 3);
//    assertEquals(result.failedPartitions.get(0).get(0), "20200220");
//    assertEquals(result.failedPartitions.get(1).get(0), "20200221");
//    assertEquals(result.failedPartitions.get(2).get(0), "20200222");
//  }
//}
