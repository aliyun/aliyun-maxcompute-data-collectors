package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class Task {

  private static final Logger LOG = LogManager.getLogger(Task.class);
  protected long updateTime;
  protected Map<Action, ActionInfo> actionInfoMap;
  protected Progress progress;
  MetaSource.TableMetaModel tableMetaModel;
  MetaConfiguration.Config tableConfig;

  public Task(MetaSource.TableMetaModel tableMetaModel,
              MetaConfiguration.Config tableConfig) {
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

  // TODO: has side effects, will change action status and task status as well
  protected void changeExecutionProgress(Action action, String executionTaskName, Progress newProgress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }
    ActionInfo actionInfo = actionInfoMap.get(action);
    Map<String, AbstractExecutionInfo> executionInfoMap = actionInfo.executionInfoMap;
    if (!executionInfoMap.containsKey(executionTaskName)) {
      return;
    }

    // TODO: is this necessary?
    if (newProgress.equals(executionInfoMap.get(executionTaskName).progress)) {
      return;
    }

    // step1. update execution.progress
    // step2. update action.progress
    // step3. update task.progress

    executionInfoMap.get(executionTaskName).progress = newProgress;

    boolean actionProgressChanged = updateActionProgress(actionInfo, newProgress);

    if (actionProgressChanged) {
      updateTaskProgress(actionInfo.progress);
    }
  }

  protected void changeActionProgress(Action action, Progress newProgress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }
    ActionInfo actionInfo = actionInfoMap.get(action);
    boolean actionProgressChanged = updateActionProgress(actionInfo, newProgress);

    if (actionProgressChanged) {
      updateTaskProgress(actionInfo.progress);
    }
  }

  private boolean updateActionProgress(ActionInfo actionInfo, Progress executionProgress) {
    boolean actionProgressChanged = false;
    if (Progress.SUCCEEDED.equals(executionProgress)) {
      // Check if all subtask has succeeded, set the task status to succeeded if true
      if (!Progress.SUCCEEDED.equals(actionInfo.progress)
          && actionInfo.executionInfoMap.values().stream().allMatch(v -> v.progress.equals(Progress.SUCCEEDED))) {
        actionInfo.progress = Progress.SUCCEEDED;
        actionProgressChanged = true;
      }
    } else if (Progress.FAILED.equals(executionProgress)) {
      // Set the task status to failed if any subtask failed
      if (!Progress.FAILED.equals(actionInfo.progress)) {
        actionInfo.progress = Progress.FAILED;
        actionProgressChanged = true;
      }
    } else if (Progress.RUNNING.equals(executionProgress)) {
      // TODO: not very intuitive, should change the task status to running when it is scheduled
      if (Progress.NEW.equals(actionInfo.progress)) {
        actionInfo.progress = Progress.RUNNING;
        actionProgressChanged = true;
      }
    }
    return actionProgressChanged;
  }

  private void updateTaskProgress(Progress actionProgress) {
    if (Progress.SUCCEEDED.equals(actionProgress)) {
      if (!Progress.SUCCEEDED.equals(this.progress)
          && actionInfoMap.values().stream().allMatch(v -> v.progress.equals(Progress.SUCCEEDED))) {
        this.progress = Progress.SUCCEEDED;
      }
    } else if (Progress.FAILED.equals(actionProgress)) {
      if (!Progress.FAILED.equals(this.progress)) {
        this.progress = Progress.FAILED;
      }
    } else if (Progress.RUNNING.equals(actionProgress)) {
      if (Progress.NEW.equals(this.progress)) {
        this.progress = Progress.RUNNING;
      }
    }
  }

  /**
   * Is all actions should be executed before given action already succeeded.
   * @param action
   * @return
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
      if (entry.getKey().ordinal() < action.ordinal() &&
          !Progress.SUCCEEDED.equals(entry.getValue().progress)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    return "[" + tableMetaModel.databaseName + "." + tableMetaModel.tableName + "]";
  }

  public String getSourceDatabaseName() {
    return tableMetaModel.databaseName;
  }

  public String getSourceTableName() {
    return tableMetaModel.tableName;
  }

  public String getName() {
    return tableMetaModel.databaseName + "." + tableMetaModel.tableName;
  }
}
