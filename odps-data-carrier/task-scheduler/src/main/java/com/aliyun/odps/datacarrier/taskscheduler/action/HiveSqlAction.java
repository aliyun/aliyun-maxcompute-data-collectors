package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.executor.ActionExecutorFactory;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.HiveSqlActionInfo;

abstract class HiveSqlAction extends AbstractAction {

  private static final Logger LOG = LogManager.getLogger(HiveSqlAction.class);

  Future<List<List<String>>> future;

  public HiveSqlAction(String id) {
    super(id);
    actionInfo = new HiveSqlActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);

    this.future = ActionExecutorFactory
        .getHiveSqlExecutor()
        .execute(getSql(), id, (HiveSqlActionInfo) actionInfo);
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

  @Override
  public void stop() {
    // TODO: try to stop hive sql
  }

  abstract String getSql();

  // TODO: should have an abstract method getSettings
}
