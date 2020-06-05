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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: should be more general, not just between hive and odps
// TODO: UT
public class VerificationRunner implements TaskRunner {
  private static final Logger LOG = LogManager.getLogger(VerificationRunner.class);

  @Override
  public void submitExecutionTask(Task task, Action action) throws MmaException {
    if (!task.actionInfoMap.containsKey(Action.HIVE_SOURCE_VERIFICATION)
        || !task.actionInfoMap.containsKey(Action.ODPS_SOURCE_VERIFICATION)) {
      LOG.warn("Can not find ODPS/Hive verification results of task {}, verification skipped",
               task.getName());
    }

    List<List<String>> destinationTableResult =
        ((OdpsActionInfo) task.actionInfoMap.get(Action.ODPS_DESTINATION_VERIFICATION)).getResult();
    List<List<String>> sourceTableResult = getSourceTableVerificationResult(task);

    boolean compareResult;
    if (destinationTableResult == null || sourceTableResult == null) {
      compareResult = false;
      LOG.error("Can not find ODPS/Hive verification results of task {}, verification skipped",
                task.getName());
    } else {
      if (task.tableMetaModel.partitionColumns.isEmpty()) {
        compareResult = compareNonPartitionedTableResult(task.getName(),
                                                         destinationTableResult,
                                                         sourceTableResult);
      } else {
        compareResult = comparePartitionedTableResult(task,
                                                      destinationTableResult,
                                                      sourceTableResult);
      }
    }
    if (compareResult) {
      task.updateActionProgress(action, Progress.SUCCEEDED);
    } else {
      task.updateActionProgress(action, Progress.FAILED);
    }
  }

  private List<List<String>> getSourceTableVerificationResult(Task task) {
    List<List<String>> result = null;
    if (task.actionInfoMap.containsKey(Action.HIVE_SOURCE_VERIFICATION)) {
      result = ((HiveActionInfo) task.actionInfoMap.get(Action.HIVE_SOURCE_VERIFICATION)).getResult();
    } else if (task.actionInfoMap.containsKey(Action.ODPS_SOURCE_VERIFICATION)) {
      result = ((OdpsActionInfo) task.actionInfoMap.get(Action.ODPS_SOURCE_VERIFICATION)).getResult();
    }
    return result;
  }

  private boolean compareNonPartitionedTableResult(
      String taskName,
      List<List<String>> odpsResult,
      List<List<String>> hiveResult) {
    if (odpsResult.size() == hiveResult.size() && odpsResult.size() == 1) {
      long hiveTableCount = Long.parseLong(hiveResult.get(0).get(0));
      long odpsTableCount = Long.parseLong(odpsResult.get(0).get(0));
      if (hiveTableCount == odpsTableCount) {
        LOG.info("Table {} pass data verification, hive table count: {}, odps table count: {}",
                 taskName, hiveTableCount, odpsTableCount);
        return true;
      } else {
        LOG.info("Table {} failed data verification, hive table count: {}, odps table count: {}.",
                 taskName, hiveTableCount, odpsTableCount);
        return false;
      }
    }
    return false;
  }

  private boolean comparePartitionedTableResult(
      Task task,
      List<List<String>> odpsResult,
      List<List<String>> hiveResult) {
    VerificationActionInfo actionInfo =
        (VerificationActionInfo) task.actionInfoMap.get(Action.VERIFICATION);
    List<List<String>> succeededPartitions = new LinkedList<>();
    List<List<String>> failedPartitions = new LinkedList<>();

    int partitionColumnCount = task.tableMetaModel.partitionColumns.size();

    Map<String, Long> odpsPartitionCounts = new HashMap<>();
    for (List<String> record : odpsResult) {
      StringBuilder partitionValues = new StringBuilder();
      for (int i = 0; i < partitionColumnCount; i++) {
        if (i != 0) {
          partitionValues.append(", ");
        }
        partitionValues.append(record.get(i));
      }
      odpsPartitionCounts.put(partitionValues.toString(),
                              Long.valueOf(record.get(partitionColumnCount)));
    }

    Map<String, Long> hivePartitionCounts = new HashMap<>();
    for (List<String> record : hiveResult) {
      StringBuilder partitionValues = new StringBuilder();
      for (int i = 0; i < partitionColumnCount; i++) {
        if (i != 0) {
          partitionValues.append(", ");
        }
        partitionValues.append(record.get(i));
      }
      hivePartitionCounts.put(partitionValues.toString(),
                              Long.valueOf(record.get(partitionColumnCount)));
    }

    // Compare results
    boolean result = true;
    for (MetaSource.PartitionMetaModel partitionMetaModel : task.tableMetaModel.partitions) {
      String partitionValues = String.join(", ", partitionMetaModel.partitionValues);

      if (!odpsPartitionCounts.containsKey(partitionValues)
          && !hivePartitionCounts.containsKey(partitionValues)) {
        // If a partition is empty, it will not be in the result set
        LOG.info("Partition verification passed, partition: {} is empty", partitionValues);
      } else if (odpsPartitionCounts.containsKey(partitionValues)
                 && hivePartitionCounts.containsKey(partitionValues)) {
        long odpsRecordCount = odpsPartitionCounts.get(partitionValues);
        long hiveRecordCount = hivePartitionCounts.get(partitionValues);

        if (odpsRecordCount == hiveRecordCount) {
          LOG.info("Partition verification passed, partition: {} , record count: {}",
                   partitionValues, odpsRecordCount);
          succeededPartitions.add(partitionMetaModel.partitionValues);
        } else {
          LOG.warn("Partition verification failed, partition: {}, hive: {}, ODPS: {}",
                   partitionValues, hiveRecordCount, odpsRecordCount);
          failedPartitions.add(partitionMetaModel.partitionValues);
          result = false;
        }
      } else {
        if (!odpsPartitionCounts.containsKey(partitionValues)) {
          LOG.warn("Partition verification failed, partition: {} is not found in ODPS",
                   partitionValues);
        } else {
          LOG.warn("Partition verification failed, partition: {} is not found in Hive "
                   + "(probably due to changes during migration)", partitionValues);
        }
        failedPartitions.add(partitionMetaModel.partitionValues);
        result = false;
      }

      actionInfo.setSucceededPartitions(succeededPartitions);
      actionInfo.setFailedPartitions(failedPartitions);
    }

    return result;
  }

  @Override
  public void shutdown() {
  }
}
