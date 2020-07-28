package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_FUNCTION_FOLDER;

public class OdpsExportFunctionAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportFunctionAction.class);

  private String taskName;
  private Function function;

  public OdpsExportFunctionAction(String id, String taskName, Function func) {
    super(id);
    this.taskName = taskName;
    this.function = func;
  }

  @Override
  public void doAction() throws MmaException {
    try {
      List<String> resources = new ArrayList<>();
      for (Resource resource : function.getResources()) {
        resources.add(resource.getName());
      }
      OdpsFunctionInfo functionInfo = new OdpsFunctionInfo(function.getName(), function.getClassPath(), resources);
      String ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_FUNCTION_FOLDER,
          function.getProject(),
          function.getName(),
          EXPORT_META_FILE_NAME);
      String content = GsonUtils.toJson(functionInfo);
      LOG.info("Task: {}, function info: {}", id, content);
      OssUtils.createFile(ossFileName, content);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
          id, ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
