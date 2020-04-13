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

import com.aliyun.odps.data.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class VerificationRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(VerificationRunner.class);

  @Override
  public void submitExecutionTask(Task task, Action action) throws MmaException {
    if (!(task.actionInfoMap.containsKey(Action.HIVE_SOURCE_VERIFICATION) &&
          task.actionInfoMap.containsKey(Action.ODPS_SOURCE_VERIFICATION))||
        !task.actionInfoMap.containsKey(Action.ODPS_DESTINATION_VERIFICATION)) {
      LOG.warn("Can not find ODPS/Hive verification tasks, skip verification task {} result.", task.getName());
    }

    List<Record> destinationTableResult =
        ((OdpsActionInfo) task.actionInfoMap.get(Action.ODPS_DESTINATION_VERIFICATION)).getInfos().iterator().next().getResult();
    List<List<String>> sourceTableResult = getSourceTableVerificationResult(task);

    boolean compareResult;
    if (destinationTableResult == null || sourceTableResult == null) {
      compareResult = false;
      LOG.error("Can not find ODPS/Hive verification results, verification task {} failed.", task.getName());
    } else {
      if (task.tableMetaModel.partitionColumns.isEmpty()) {
        compareResult = compareNonPartitionedTableResult(task.getName(), destinationTableResult, sourceTableResult);
      } else {
        compareResult = comparePartitionedTableResult(task, destinationTableResult, sourceTableResult);
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
      result = new ArrayList<>();
      List<Record> records = ((OdpsActionInfo) task.actionInfoMap.get(Action.ODPS_SOURCE_VERIFICATION))
          .getInfos().iterator().next().getResult();
      for (Record record : records) {
        List<String> row = new ArrayList<>();
        for (int i = 0; i < record.getColumnCount(); i++) {
          row.add(record.get(i).toString());
        }
        result.add(row);
      }
    }
    return result;
  }

  private boolean compareNonPartitionedTableResult(String taskName, List<Record> odpsResult, List<List<String>> hiveResult) {
    if (odpsResult.size() == hiveResult.size() && odpsResult.size() == 1) {
      long hiveTableCount = Long.parseLong(hiveResult.get(0).get(0));
      long odpsTableCount = Long.parseLong(odpsResult.get(0).get(0).toString());
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

  private boolean comparePartitionedTableResult(Task task, List<Record> odpsResult, List<List<String>> hiveResult) {
    boolean result = true;
    int partitionColumnCount = task.tableMetaModel.partitionColumns.size();

    Map<String, Long> odpsPartitionCounts = new HashMap<>();
    for (Record record : odpsResult) {
      StringBuilder partitionValues = new StringBuilder();
      for (int i = 0; i < partitionColumnCount; i++) {
        if (i != 0) {
          partitionValues.append(", ");
        }
        partitionValues.append(record.getString(i));
      }
      odpsPartitionCounts.put(partitionValues.toString(),
                              Long.valueOf(record.get(partitionColumnCount).toString()));
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
    Iterator<MetaSource.PartitionMetaModel> partitionValueIter =
        task.tableMetaModel.partitions.iterator();
    while (partitionValueIter.hasNext()) {
      MetaSource.PartitionMetaModel partitionMetaModel = partitionValueIter.next();
      String partitionValues = String.join(", ", partitionMetaModel.partitionValues);

      if (!odpsPartitionCounts.containsKey(partitionValues)
          && !hivePartitionCounts.containsKey(partitionValues)) {
        // If a partition is empty, it will not be in the result set
        LOG.info("Partition verification passed, partition: {} is empty", partitionValues);
        continue;
      } else if (odpsPartitionCounts.containsKey(partitionValues)
                 && hivePartitionCounts.containsKey(partitionValues)) {
        long odpsRecordCount = odpsPartitionCounts.get(partitionValues);
        long hiveRecordCount = hivePartitionCounts.get(partitionValues);

        if(odpsRecordCount == hiveRecordCount) {
          LOG.info("Partition verification passed, partition: {} , record count: {}",
                   partitionValues, odpsRecordCount);
          continue;
        } else {
          LOG.warn("Partition verification failed, partition: {}, hive: {}, ODPS: {}",
                   partitionValues, hiveRecordCount, odpsRecordCount);
        }
      } else {
        if (!odpsPartitionCounts.containsKey(partitionValues)) {
          LOG.warn("Partition verification failed, partition: {} is not found in ODPS",
                   partitionValues);
        } else {
          LOG.warn("Partition verification failed, partition: {} is not found in Hive "
                   + "(probably due to changes during migration)", partitionValues);
        }
      }

      // Remove failed partitions from TableMetaModel, so that they won't be updated to succeeded
      partitionValueIter.remove();
      result = false;
    }

    return result;
  }

  @Override
  public void shutdown() {

  }
}
