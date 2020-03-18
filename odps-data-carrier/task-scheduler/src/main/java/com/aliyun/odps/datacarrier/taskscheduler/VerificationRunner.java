package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.data.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VerificationRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(VerificationRunner.class);

  @Override
  public void submitExecutionTask(Task task, Action action) {
    if (!task.actionInfoMap.containsKey(Action.HIVE_VERIFICATION) ||
        !task.actionInfoMap.containsKey(Action.ODPS_VERIFICATION)) {
      LOG.warn("Can not find ODPS/Hive verification tasks, skip verification task {} result.", task.getName());
    }

    List<Record> odpsResult =
        ((OdpsActionInfo) task.actionInfoMap.get(Action.ODPS_VERIFICATION)).getInfos().iterator().next().getResult();
    List<List<String>> hiveResult = ((HiveActionInfo) task.actionInfoMap.get(Action.HIVE_VERIFICATION)).getResult();

    boolean compareResult;
    if (odpsResult == null || odpsResult.isEmpty() || hiveResult == null) {
      compareResult = false;
      LOG.error("Can not find ODPS/Hive verification results, verification task {} failed.", task.getName());
    } else {
      if (task.tableMetaModel.partitionColumns.isEmpty()) {
        compareResult = compareNonPartitionedTableResult(task.getName(), odpsResult, hiveResult);
      } else {
        compareResult = comparePartitionedTableResult(task, odpsResult, hiveResult);
      }
    }
    if (compareResult) {
      task.updateActionProgress(action, Progress.SUCCEEDED);
    } else {
      task.updateActionProgress(action, Progress.FAILED);
    }

  }

  private boolean compareNonPartitionedTableResult(String taskName, List<Record> odpsResult, List<List<String>> hiveResult) {
    if (odpsResult.size() == hiveResult.size() && odpsResult.size() == 1) {
      long hiveTableCount = Long.valueOf(hiveResult.get(0).get(0));
      long odpsTableCount = Long.valueOf(odpsResult.get(0).get(0).toString());
      if (hiveTableCount == odpsTableCount) {
        LOG.info("Table {} pass data verification, count: {}.", taskName, hiveTableCount);
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
    int partitionColumnsCount = task.tableMetaModel.partitionColumns.size();
    Map<String, Long> odpsPartitionCounts = new HashMap<>();
    for (Record record : odpsResult) {
      StringBuilder partitionValue = new StringBuilder();
      for (int i = 0; i < partitionColumnsCount; i++) {
        if (i != 0) {
          partitionValue.append(", ");
        }
        partitionValue.append(record.get(i).toString());
      }
      odpsPartitionCounts.put(partitionValue.toString(), Long.valueOf(record.get(partitionColumnsCount).toString()));
    }

    Map<String, Long> hivePartitionCounts = new HashMap<>();
    for (List<String> record : hiveResult) {
      StringBuilder partitionValue = new StringBuilder();
      for (int i = 0; i < partitionColumnsCount; i++) {
        if (i != 0) {
          partitionValue.append(", ");
        }
        partitionValue.append(record.get(i));
      }
      hivePartitionCounts.put(partitionValue.toString(), Long.valueOf(record.get(partitionColumnsCount)));
    }

    //validate partition result.
    Iterator<MetaSource.PartitionMetaModel> partitionValueItor = task.tableMetaModel.partitions.iterator();
    while (partitionValueItor.hasNext()) {
      MetaSource.PartitionMetaModel partitionMetaModel = partitionValueItor.next();
      String partitionValue = partitionMetaModel.partitionValues.stream().collect(Collectors.joining(", "));
      if (odpsPartitionCounts.containsKey(partitionValue) && hivePartitionCounts.containsKey(partitionValue)) {
          if(odpsPartitionCounts.get(partitionValue).longValue() == hivePartitionCounts.get(partitionValue).longValue()) {
            LOG.info("Partition value {} pass validation, count: {}",
                partitionValue, odpsPartitionCounts.get(partitionValue).longValue());
            continue;
          } else {
            LOG.warn("Partition count is not equal, partition values: {}, hive count: {}, odps count: {}",
                partitionValue, hivePartitionCounts.get(partitionValue), odpsPartitionCounts.get(partitionValue));
          }
      } else {
        LOG.warn("Partition value {} is not found in ODPS/Hive results.", partitionValue);
      }
      partitionValueItor.remove();
      result = false;
    }
    return result;
  }

  @Override
  public void shutdown() {

  }
}
