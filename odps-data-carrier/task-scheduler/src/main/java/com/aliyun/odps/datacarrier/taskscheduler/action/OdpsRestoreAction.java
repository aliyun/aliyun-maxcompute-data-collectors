package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class OdpsRestoreAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestoreAction.class);

  private MmaConfig.ObjectRestoreConfig restoreConfig;

  public OdpsRestoreAction(String id, MmaConfig.ObjectRestoreConfig restoreConfig) {
    super(id);
    this.restoreConfig = restoreConfig;
  }

  public String getTaskName() {
    return restoreConfig.getTaskName();
  }

  public String getOriginProject() {
    return restoreConfig.getOriginDatabaseName();
  }

  public String getDestinationProject() {
    return restoreConfig.getDestinationDatabaseName();
  }

  public MmaConfig.ObjectType getObjectType() {
    return restoreConfig.getObjectType();
  }

  public String getObjectName() {
    return restoreConfig.getObjectName();
  }

  public boolean isUpdate() {
    return restoreConfig.isUpdate();
  }

  @Override
  public void doAction() throws MmaException {
    try {
      restore();
    } catch (Exception e) {
      LOG.error("Action {} failed, restore type: {} object: {} from {} to {}, update {}, stack trace: {}",
          id, restoreConfig.getObjectType(), restoreConfig.getObjectName(), restoreConfig.getOriginDatabaseName(),
          restoreConfig.getDestinationDatabaseName(), restoreConfig.isUpdate(), ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  abstract void restore() throws Exception;

  public String getRestoredFilePath(String folderName, String fileName) throws Exception {
    String ossFileName = OssUtils.getOssPathToExportObject(getTaskName(),
        folderName,
        getOriginProject(),
        getObjectName(),
        fileName);
    if (!OssUtils.exists(ossFileName)) {
      LOG.error("Oss file {} not found", ossFileName);
      throw new MmaException("Oss file " + ossFileName + " not found when restore " + getObjectType().name()
          + " " + getObjectName() + " from " + getOriginProject() + " to " + getDestinationProject());
    }
    return ossFileName;
  }
}
