package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Objects;

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
