package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class Task {

  private static final Logger LOG = LogManager.getLogger(Task.class);
  private String taskName;
  protected long updateTime;
  protected Map<Action, AbstractExecutionInfo> actionInfoMap;
  protected Progress progress;
  MetaSource.TableMetaModel tableMetaModel;
  MetaConfiguration.Config tableConfig;

  public Task(String taskName, MetaSource.TableMetaModel tableMetaModel, MetaConfiguration.Config tableConfig) {
    this.taskName = taskName;
    this.tableMetaModel = tableMetaModel;
    this.tableConfig = tableConfig;
    this.updateTime = System.currentTimeMillis();
    this.actionInfoMap = new ConcurrentHashMap<>();
    this.progress = Progress.NEW;
  }

  protected void addExecutionInfo(Action action) {
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    if (RunnerType.ODPS.equals(runnerType)) {
      actionInfoMap.put(action, new OdpsExecutionInfo());
    } else if (RunnerType.HIVE.equals(runnerType)) {
      actionInfoMap.put(action, new HiveExecutionInfo());
    } else if (RunnerType.VALIDATOR.equals(runnerType)) {
      actionInfoMap.put(action, new ValidateExecutionInfo());
    }
  }

  /**
   * Update execution progress, will trigger a action progress update
   * @param action action that the execution belongs to
   * @param progress new progress
   */
  protected synchronized void updateActionExecutionProgress(Action action,
                                                            Progress progress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }

    AbstractExecutionInfo executionInfo = actionInfoMap.get(action);
    if (!executionInfo.progress.equals(progress)) {
      executionInfo.progress = progress;
      updateTaskProgress(executionInfo.progress);
    }
  }

  /**
   * Update task progress, triggered by an action progress update
   * @param actionNewProgress the new action progress that triggers this update
   */
  private void updateTaskProgress(Progress actionNewProgress) {
    boolean taskProgressChanged = false;

    switch (progress) {
      case NEW:
      case RUNNING:
        if (Progress.RUNNING.equals(actionNewProgress)) {
          progress = Progress.RUNNING;
          taskProgressChanged = true;
        } else if (Progress.FAILED.equals(actionNewProgress)) {
          progress = Progress.FAILED;
          taskProgressChanged = true;
        } else if (Progress.SUCCEEDED.equals(actionNewProgress)) {
          boolean allActionsSucceeded = actionInfoMap.values()
              .stream().allMatch(v -> v.progress.equals(Progress.SUCCEEDED));
          if (allActionsSucceeded) {
            progress = Progress.SUCCEEDED;
            taskProgressChanged = true;
          }
        }
        break;
      case FAILED:
      case SUCCEEDED:
      default:
    }

    // No need to update the table status when succeeded, since succeeded is default value
    if (taskProgressChanged) {
      if (Progress.FAILED.equals(progress)) {
        TaskScheduler.markAsFailed(tableMetaModel.databaseName, tableMetaModel.tableName);
      }
      //Partitioned table should update partition values when task.progress is Succeeded or failed in Action.VALIDATE.
      if (!tableMetaModel.partitionColumns.isEmpty()) {
        if (Progress.SUCCEEDED.equals(progress) ||
            (Progress.FAILED.equals(progress) &&
                actionInfoMap.containsKey(Action.VALIDATE) &&
                actionInfoMap.get(Action.VALIDATE).progress.equals(Progress.FAILED))) {
          List<List<String>> partitionValuesList = tableMetaModel.partitions
              .stream()
              .map(p -> p.partitionValues)
              .collect(Collectors.toList());
          MMAMetaManagerFsImpl.getInstance().updateStatus(tableMetaModel.databaseName,
              tableMetaModel.tableName,
              partitionValuesList,
              MMAMetaManager.MigrationStatus.SUCCEEDED);
        }
      }
    }
  }

  /**
   * If all parent actions succeeded
   * @param action action
   * @return returns true if all parent actions succeeded, else false
   */
  public boolean isReadyAction(Action action) {
    if (!actionInfoMap.containsKey(action)) {
      return false;
    }

    // If is action is already scheduled
    if (!Progress.NEW.equals(actionInfoMap.get(action).progress)) {
      return false;
    }

    // If its previous actions have finished successfully
    for (Map.Entry<Action, AbstractExecutionInfo> entry : actionInfoMap.entrySet()) {
      // TODO: quite hacky
      if (entry.getKey().getPriority() < action.getPriority() &&
          !Progress.SUCCEEDED.equals(entry.getValue().progress)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    return taskName;
  }


  public String getSourceDatabaseName() {
    return tableMetaModel.databaseName;
  }

  public String getSourceTableName() {
    return tableMetaModel.tableName;
  }

  public String getName() {
    return taskName;
  }
}
