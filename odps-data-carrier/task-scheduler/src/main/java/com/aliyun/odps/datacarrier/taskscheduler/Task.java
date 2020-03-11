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
  protected Map<Action, ActionInfo> actionInfoMap;
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

  static class ActionInfo {
    protected Progress progress = Progress.NEW;
    protected Map<String, AbstractExecutionInfo> executionInfoMap = new ConcurrentHashMap<>();
  }

  protected void addActionInfo(Action action) {
    actionInfoMap.putIfAbsent(action, new ActionInfo());
  }

  protected void addExecutionInfo(Action action, String executionTaskName, AbstractExecutionInfo executionInfo) {
    ActionInfo actionInfo = actionInfoMap.computeIfAbsent(action, k -> new ActionInfo());
    if (actionInfo.executionInfoMap.containsKey(executionTaskName)) {
      LOG.warn("Execution task already exists, create failed, " +
          "Action: " + action.name() +
          "TaskName: " + executionTaskName);
      return;
    }
    actionInfo.executionInfoMap.put(executionTaskName, executionInfo);
  }

  protected void addExecutionInfo(Action action, String executionTaskName) {
    ActionInfo actionInfo = actionInfoMap.computeIfAbsent(action, k -> new ActionInfo());
    if (actionInfo.executionInfoMap.containsKey(executionTaskName)) {
      LOG.warn("Execution task already exists, create failed, " +
          "Action: " + action.name() +
          "TaskName: " + executionTaskName);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    if (RunnerType.ODPS.equals(runnerType)) {
      actionInfo.executionInfoMap.put(executionTaskName, new OdpsExecutionInfo());
    } else if (RunnerType.HIVE.equals(runnerType)) {
      actionInfo.executionInfoMap.put(executionTaskName, new HiveExecutionInfo());
    }
  }

  /**
   * Update execution progress, will trigger a action progress update
   * @param action action that the execution belongs to
   * @param executionTaskName execution name
   * @param progress new progress
   */
  protected synchronized void updateExecutionProgress(Action action,
                                                      String executionTaskName,
                                                      Progress progress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }

    ActionInfo actionInfo = actionInfoMap.get(action);
    if (!actionInfo.executionInfoMap.containsKey(executionTaskName)) {
      return;
    }
    if (actionInfo.executionInfoMap.get(executionTaskName).progress.equals(progress)) {
      return;
    }

    // TODO: should check if the progress change is valid or not
    actionInfo.executionInfoMap.get(executionTaskName).progress = progress;

    // Triggers an action progress update
    updateActionProgress(action, progress);
  }

  /**
   * Update action progress, triggered by a execution progress update
   * @param action action
   * @param executionNewProgress the new execution progress that triggers this update
   */
  private void updateActionProgress(Action action, Progress executionNewProgress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }

    ActionInfo actionInfo = actionInfoMap.get(action);
    boolean actionProgressChanged = false;

    switch (actionInfo.progress) {
      case NEW:
      case RUNNING:
        if (Progress.RUNNING.equals(executionNewProgress)) {
          actionInfo.progress = Progress.RUNNING;
          actionProgressChanged = true;
        } else if (Progress.FAILED.equals(executionNewProgress)) {
          actionInfo.progress = Progress.FAILED;
          actionProgressChanged = true;
        } else if (Progress.SUCCEEDED.equals(executionNewProgress)) {
          boolean allExecutionSucceeded = actionInfo.executionInfoMap.values()
              .stream().allMatch(v -> v.progress.equals(Progress.SUCCEEDED));
          if (allExecutionSucceeded) {
            actionInfo.progress = Progress.SUCCEEDED;
            actionProgressChanged = true;
          }
        }
        break;
      case FAILED:
      case SUCCEEDED:
      default:
    }

    // Triggers a task progress update
    if (actionProgressChanged) {
      updateTaskProgress(actionInfo.progress);
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
      } else if (Progress.SUCCEEDED.equals(progress)) {
        // Partitioned table should update succeeded partitions
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
    for (Map.Entry<Action, ActionInfo> entry : actionInfoMap.entrySet()) {
      // TODO: quite hacky
      if (entry.getKey().ordinal() < action.ordinal() &&
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
