package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

public class TableSplitter implements TaskManager {

  private static final Logger LOG = LogManager.getLogger(TableSplitter.class);
  private List<MetaSource.TableMetaModel> tables;
  private List<Task> tasks = new LinkedList<>();

  public TableSplitter(List<MetaSource.TableMetaModel> tables) {
    this.tables = tables;
  }

  @Override
  public List<Task> generateTasks(SortedSet<Action> actions) {
    for (MetaSource.TableMetaModel tableMetaModel : this.tables) {

      MetaConfiguration.Config config =  MMAMetaManagerFsImpl
          .getInstance()
          .getConfig(tableMetaModel.databaseName, tableMetaModel.tableName).config;

      // Empty partitions, means the table is non-partition table.
      if (tableMetaModel.partitionColumns.isEmpty()) {
        tasks.add(generateTaskForNonPartitionedTable(tableMetaModel, config, actions));
      } else {
        tasks.addAll(generateTaskForPartitionedTable(tableMetaModel, config, actions));
      }
    }

    LOG.warn("Tasks: {}",
             tasks.stream().map(Task::getName).collect(Collectors.joining(", ")));

    return this.tasks;
  }

  @VisibleForTesting
  protected Task generateTaskForNonPartitionedTable(MetaSource.TableMetaModel tableMetaModel,
                                                    MetaConfiguration.Config config,
                                                    SortedSet<Action> actions) {
    String taskName = tableMetaModel.databaseName + "." + tableMetaModel.tableName;
    Task task = new Task(taskName, tableMetaModel, config);
    for (Action action : actions) {
      if (Action.ODPS_ADD_PARTITION.equals(action)) {
        continue;
      }
      task.addExecutionInfo(action, taskName);
    }
    return task;
  }

  @VisibleForTesting
  protected List<Task> generateTaskForPartitionedTable(MetaSource.TableMetaModel tableMetaModel,
                                                       MetaConfiguration.Config config,
                                                       SortedSet<Action> actions) {
    List<Task> ret = new LinkedList<>();

    int numOfPartitions = -1;

    if (config != null && config.getNumOfPartitions() > 0) {
      numOfPartitions = config.getNumOfPartitions();
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
      String taskName = tableMetaModel.databaseName + "." + tableMetaModel.tableName +
                        "." + taskIndex;
      Task task = new Task(taskName, clone, config);
      // TODO: style
      for (int partitionIndex = taskIndex * numPartitionsPerSet;
           partitionIndex < (taskIndex + 1) * numPartitionsPerSet
           && partitionIndex < numOfAllPartitions;
           partitionIndex++) {
        task.tableMetaModel.partitions.add(tableMetaModel.partitions.get(partitionIndex));
      }
      for (Action action : actions) {
        task.addExecutionInfo(action, taskName);
      }
      ret.add(task);
    }
    return ret;
  }
}
