package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;

public class OdpsDestVerificationAction extends OdpsSqlAction {

  private static final Logger LOG = LogManager.getLogger(OdpsDestVerificationAction.class);

  public OdpsDestVerificationAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getVerifySql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    // TODO: should use table migration config
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getVerifySettings();
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = (List<List<String>>) future.get();
      actionExecutionContext.setDestVerificationResult(rows);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
