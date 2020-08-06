package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.executor.ActionExecutorFactory;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;

public abstract class OdpsNoSqlAction extends AbstractAction {

  public OdpsNoSqlAction(String id) {
    super(id);
    actionInfo = new OdpsSqlActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);
    future = ActionExecutorFactory.getOdpsExecutor().execute(this);
  }

  public abstract void doAction() throws MmaException;

  @Override
  public void stop() {
  }
}
