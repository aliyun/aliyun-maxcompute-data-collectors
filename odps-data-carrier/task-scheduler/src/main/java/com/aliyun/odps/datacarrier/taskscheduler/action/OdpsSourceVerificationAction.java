package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;

public class OdpsSourceVerificationAction extends OdpsSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveSourceVerificationAction.class);

  public OdpsSourceVerificationAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getVerifySql(actionExecutionContext.getTableMetaModel(), false);
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getSourceTableSettings()
        .getVerifySettings();
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = (List<List<String>>) future.get();
      actionExecutionContext.setSourceVerificationResult(rows);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id,
                ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
