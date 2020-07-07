package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.executor.ActionExecutorFactory;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;

abstract class OdpsSqlAction extends AbstractAction {

  public OdpsSqlAction(String id) {
    super(id);
    actionInfo = new OdpsSqlActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);

    this.future = ActionExecutorFactory
        .getOdpsSqlExecutor()
        .execute(getSql(), getSettings(), id, (OdpsSqlActionInfo) actionInfo);
  }

  @Override
  public void stop() {
  }

  abstract String getSql();

  abstract Map<String, String> getSettings();
}
