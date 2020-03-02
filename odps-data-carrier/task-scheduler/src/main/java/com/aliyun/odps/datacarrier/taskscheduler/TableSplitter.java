package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

public class TableSplitter implements TaskManager {

  private static final Logger LOG = LogManager.getLogger(TableSplitter.class);
  private List<MetaSource.TableMetaModel> tables;
  private List<Task> tasks = new LinkedList<>();
  private MetaConfiguration metaConfiguration;

  public TableSplitter(List<MetaSource.TableMetaModel> tables, MetaConfiguration metaConfig) {
    this.tables = tables;
    this.metaConfiguration = metaConfig;
  }

  @Override
  public List<Task> generateTasks(SortedSet<Action> actions, Mode mode) {
    for (MetaSource.TableMetaModel tableMetaModel : this.tables) {
      MetaConfiguration.Config tableConfig =
          metaConfiguration.getTableConfig(tableMetaModel.databaseName, tableMetaModel.tableName);
      // Empty partitions, means the table is non-partition table.
      if (tableMetaModel.partitionColumns.isEmpty()) {
        Task task = new Task(tableMetaModel, tableConfig);
        for (Action action : actions) {
          if (Action.ODPS_ADD_PARTITION.equals(action)) {
            continue;
          }
          task.addExecutionInfo(action, task.getName());
        }
        this.tasks.add(task);
      } else {
        int numOfPartitions = -1;

        if (tableConfig != null && tableConfig.getNumOfPartitions() > 0) {
          numOfPartitions = tableConfig.getNumOfPartitions();
        }
        int numOfAllPartitions = tableMetaModel.partitions.size();
        int numOfSplitSet = 1;
        int numPartitionsPerSet = numOfAllPartitions;
        if (numOfPartitions > 0) {
          numOfSplitSet = (numOfAllPartitions + numOfPartitions - 1) / numOfPartitions;
          numPartitionsPerSet = (numOfAllPartitions + numOfSplitSet - 1) / numOfSplitSet;
        }
        for (int taskIndex = 0; taskIndex < numOfSplitSet; taskIndex++) {
          MetaSource.TableMetaModel clone = tableMetaModel.clone();
          clone.partitions = new LinkedList<>();
          Task task = new Task(clone, tableConfig);
          for (int partitionIndex = taskIndex * numPartitionsPerSet;
               partitionIndex < (taskIndex + 1) * numPartitionsPerSet && partitionIndex < numOfAllPartitions;
               partitionIndex++) {
            task.tableMetaModel.partitions.add(tableMetaModel.partitions.get(partitionIndex));
          }
          for (Action action : actions) {
            task.addExecutionInfo(action, task.getName() + "." + taskIndex);
          }
          this.tasks.add(task);
        }
      }
    }
    return this.tasks;
  }
}
