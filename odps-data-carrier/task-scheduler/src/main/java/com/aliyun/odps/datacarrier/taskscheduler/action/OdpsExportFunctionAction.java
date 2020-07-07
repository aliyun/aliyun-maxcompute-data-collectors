package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_DDL_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_FUNCTION_DDL_FOLDER_NAME;

public class OdpsExportFunctionAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportFunctionAction.class);

  private Function function;

  public OdpsExportFunctionAction(String id, Function func) {
    super(id);
    this.function = func;
  }

  @Override
  public void doAction() throws MmaException {
    setProgress(ActionProgress.RUNNING);
    try {
      List<String> resources = new ArrayList<>();
      for (Resource resource : function.getResources()) {
        resources.add(resource.getName());
      }
      String script = OdpsSqlUtils.getCreateFunctionStatement(function.getName(), function.getClassPath(), resources);
      LOG.info("Task {}, script {}", id, script);
      String ossFileName = OssUtils.getOssPathToExportObject(
          EXPORT_FUNCTION_DDL_FOLDER_NAME,
          function.getProject(),
          function.getName(),
          EXPORT_DDL_FILE_NAME);
      OssUtils.createFile(ossFileName, script);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
          id, ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
