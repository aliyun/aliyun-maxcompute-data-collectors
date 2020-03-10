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

    // If this table doesn't have any partition, create a task an return
    if (tableMetaModel.partitions.isEmpty()) {
      String taskName = tableMetaModel.databaseName + "." + tableMetaModel.tableName;
      Task task = new Task(taskName, tableMetaModel.clone(), config);
      for (Action action : actions) {
        task.addExecutionInfo(action, taskName);
      }
      ret.add(task);
      return ret;
    }

    int partitionGroupSize;
    // By default, partition group size is number of partitions
    if (config != null && config.getPartitionGroupSize() > 0) {
      partitionGroupSize = config.getPartitionGroupSize();
    } else {
      partitionGroupSize = tableMetaModel.partitions.size();
    }

    // TODO: should do this in meta configuration
    if (partitionGroupSize <= 0) {
      throw new IllegalArgumentException("Invalid partition group size: " + partitionGroupSize);
    }

    int startIdx = 0;
    int taskIdx = 0;
    while (startIdx < tableMetaModel.partitions.size()) {
      MetaSource.TableMetaModel clone = tableMetaModel.clone();

      // Set partitions
      int endIdx = Math.min(tableMetaModel.partitions.size(), startIdx + partitionGroupSize);
      clone.partitions = new ArrayList<>(tableMetaModel.partitions.subList(startIdx, endIdx));

      String taskName =
          tableMetaModel.databaseName + "." + tableMetaModel.tableName + "." + taskIdx;
      Task task = new Task(taskName, clone, config);
      for (Action action : actions) {
        task.addExecutionInfo(action, taskName);
      }
      ret.add(task);

      startIdx += partitionGroupSize;
      taskIdx += 1;
    }

    return ret;
  }
}
