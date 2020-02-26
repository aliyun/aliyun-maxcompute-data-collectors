package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


class Task {

  private static final Logger LOG = LogManager.getLogger(Task.class);
  protected String project; //source project
  protected String tableName; //source table
  protected long updateTime;
  protected Map<Action, ActionInfo> actionInfoMap;
  protected Progress progress;
  MetaSource.TableMetaModel tableMetaModel;
  MetaConfiguration.Config tableConfig;

  public Task(String project, String tableName, MetaSource.TableMetaModel tableMetaModel, MetaConfiguration.Config tableConfig) {
    this.project = project;
    this.tableName = tableName;
    this.tableMetaModel = tableMetaModel;
    this.tableConfig = tableConfig;
    this.updateTime = System.currentTimeMillis();
    this.actionInfoMap = new ConcurrentHashMap<>();
    this.progress = Progress.NEW;
  }

  class ActionInfo {
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

  protected void changeExecutionProgress(Action action, String executionTaskName, Progress newProgress) {
    if (!actionInfoMap.containsKey(action)) {
      return;
    }
    ActionInfo actionInfo = actionInfoMap.get(action);
    Map<String, AbstractExecutionInfo> executionInfoMap = actionInfo.executionInfoMap;
    if (!executionInfoMap.containsKey(executionTaskName)) {
      return;
    }
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
      if (!Progress.SUCCEEDED.equals(actionInfo.progress)
          && actionInfo.executionInfoMap.values().stream().allMatch(v -> v.progress.equals(Progress.SUCCEEDED))) {
        actionInfo.progress = Progress.SUCCEEDED;
        actionProgressChanged = true;
      }
    } else if (Progress.FAILED.equals(executionProgress)) {
      if (!Progress.FAILED.equals(actionInfo.progress)) {
        actionInfo.progress = Progress.FAILED;
        actionProgressChanged = true;
      }
    } else if (Progress.RUNNING.equals(executionProgress)) {
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

    if (Action.VALIDATION_BY_TABLE.equals(action)) {
      if (!Progress.NEW.equals(actionInfoMap.get(action).progress)) {
        return false;
      }
    } else {
      if (!Progress.NEW.equals(actionInfoMap.get(action).progress)
          && !Progress.RUNNING.equals(actionInfoMap.get(action).progress)) {
        return false;
      }
    }

    for (Map.Entry<Action, ActionInfo> entry : actionInfoMap.entrySet()) {
      if (entry.getKey().ordinal() < action.ordinal()
          && !Progress.SUCCEEDED.equals(entry.getValue().progress)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("[");
    sb.append(project).append(".").append(tableName).append("]");
    return sb.toString();
  }

  public String getTableNameWithProject() {
    final StringBuilder sb = new StringBuilder(project);
    sb.append(".").append(tableName);
    return sb.toString();
  }



  public String getProject() {
    return project;
  }

  public String getTableName() {
    return tableName;
  }
}
