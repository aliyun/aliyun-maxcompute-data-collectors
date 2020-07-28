package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_FUNCTION_FOLDER;

public class OdpsRestoreFunctionAction extends OdpsRestoreAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestoreFunctionAction.class);

  public OdpsRestoreFunctionAction(String id, MmaConfig.ObjectRestoreConfig restoreConfig) {
    super(id, restoreConfig);
  }

  @Override
  public void restore() throws Exception {
    String ossFileName = getRestoredFilePath(EXPORT_FUNCTION_FOLDER, EXPORT_META_FILE_NAME);
    String content = OssUtils.readFile(ossFileName);
    OdpsFunctionInfo functionInfo = GsonUtils.getFullConfigGson().fromJson(content, OdpsFunctionInfo.class);
    OdpsUtils.createFunction(getDestinationProject(), functionInfo, isUpdate());
    LOG.info("Restore function {} from {} to {}", functionInfo.getFunctionName(), getOriginProject(), getDestinationProject());
  }
}
