package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource.TableMetaModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

public class TableSplitter implements TaskManager {

  private static final Logger LOG = LogManager.getLogger(TableSplitter.class);
  private List<TableMetaModel> tables;
  private List<Task> tasks = new LinkedList<>();
  private MetaConfiguration metaConfiguration;

  public TableSplitter(List<TableMetaModel> tables, MetaConfiguration metaConfig) {
    this.tables = tables;
    this.metaConfiguration = metaConfig;
  }

  @Override
  public List<Task> generateTasks(SortedSet<Action> actions, Mode mode) {
    for (TableMetaModel tableMetaModel : this.tables) {
      // Empty partitions, means the table is non-partition table.
      if (tableMetaModel.partitions.isEmpty()) {
        Task task = new Task(tableMetaModel.databaseName, tableMetaModel.tableName);
        task.setPartitionTable(false);
        for (Action action : actions) {
          task.addExecutionInfo(action, task.getTableNameWithProject());
        }
        this.tasks.add(task);
      } else {
        int numOfPartitions = -1;
        MetaConfiguration.Config tableConfig = metaConfiguration.getTableConfig(tableMetaModel.databaseName, tableMetaModel.tableName);
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
          Task task = new Task(tableMetaModel.databaseName, tableMetaModel.tableName);
          task.setPartitionTable(true);
          for (int partitionIndex = taskIndex * numPartitionsPerSet;
               partitionIndex < (taskIndex + 1) * numPartitionsPerSet && partitionIndex < numOfAllPartitions;
               partitionIndex++) {
             task.addPartition(tableMetaModel.partitions.get(partitionIndex));
          }
          for (Action action : actions) {
            task.addExecutionInfo(action, task.getTableNameWithProject() + "." + taskIndex);
          }
          this.tasks.add(task);
        }
      }
    }
    return this.tasks;
  }
}
