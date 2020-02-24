package com.aliyun.odps.datacarrier.taskscheduler;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class TestDataValidator {

  @Test(timeout = 5000)
  public void testValidateTaskCountResultByPartition() {
    Task task = new Task("TestDataBase", "TestTable");
    task.addExecutionInfo(Action.HIVE_VALIDATE, task.getTableNameWithProject());
    task.addExecutionInfo(Action.ODPS_VALIDATE, task.getTableNameWithProject());
    task.partitions.addAll(TestTableSplitter.createPartitions(5));

    List<String> multiRecordResult = Lists.newArrayList(new String[]{"1", "2"});
    task.actionInfoMap.get(Action.HIVE_VALIDATE).executionInfoMap.get(
        task.getTableNameWithProject()).setMultiRecordResult(multiRecordResult);
    task.actionInfoMap.get(Action.ODPS_VALIDATE).executionInfoMap.get(
        task.getTableNameWithProject()).setMultiRecordResult(multiRecordResult);

    DataValidator dataValidator = new DataValidator();
    assertTrue(dataValidator.validateTaskCountResultByPartition(task).isEmpty());

  }
}
