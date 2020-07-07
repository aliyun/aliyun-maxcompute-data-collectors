package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.task.AbstractTask.ActionProgressListener;
import com.aliyun.odps.datacarrier.taskscheduler.task.ActionExecutionContext;

public abstract class AbstractAction implements Action {

  private static final Logger LOG = LogManager.getLogger(AbstractAction.class);

  private ActionProgress progress;
  private ActionProgressListener actionProgressListener;

  protected Future<Object> future;

  /**
   * Used by sub classes
   */
  String id;
  AbstractActionInfo actionInfo;
  ActionExecutionContext actionExecutionContext;

  public AbstractAction(String id) {
    this.id = Objects.requireNonNull(id);
    this.progress = ActionProgress.PENDING;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ActionProgress getProgress() {
    return progress;
  }

  @Override
  public AbstractActionInfo getActionInfo() {
    return actionInfo;
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      future.get();
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  @Override
  public boolean executionFinished() {
    if (ActionProgress.FAILED.equals(getProgress())
        || ActionProgress.SUCCEEDED.equals(getProgress())) {
      return true;
    }

    if (future == null) {
      throw new IllegalStateException("Action not executed, actionId: " + id);
    }

    return future.isDone();
  }

  void setProgress(ActionProgress progress) throws MmaException {
    LOG.info("Update action progress, id: {}, cur progress: {}, new progress: {}",
             id,
             this.progress,
             progress);
    this.progress = Objects.requireNonNull(progress);

    actionProgressListener.onActionProgressChanged(progress);
  }

  public void setActionProgressListener(ActionProgressListener actionProgressListener) {
    this.actionProgressListener = Objects.requireNonNull(actionProgressListener);
  }

  public void setActionExecutionContext(ActionExecutionContext actionExecutionContext) {
    this.actionExecutionContext = Objects.requireNonNull(actionExecutionContext);
  }

  @Override
  public String toString() {
    return id;
  }
}
