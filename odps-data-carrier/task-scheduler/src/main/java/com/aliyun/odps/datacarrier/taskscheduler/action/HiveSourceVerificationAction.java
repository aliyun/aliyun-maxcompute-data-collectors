package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

public class HiveSourceVerificationAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveSourceVerificationAction.class);

  public HiveSourceVerificationAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getVerifySql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = future.get();
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
