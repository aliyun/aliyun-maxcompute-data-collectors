package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_VIEW_FOLDER;

public class OdpsExportViewAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportViewAction.class);

  private String taskName;
  private String viewText;

  public OdpsExportViewAction(String id, String taskName, String viewText) {
    super(id);
    this.taskName = taskName;
    this.viewText = viewText;
  }

  @Override
  public void doAction() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    LOG.info("Task {}, export view {}.{}, viewText: {}", id, tableMetaModel.databaseName, tableMetaModel.tableName, viewText);
    String ossFileName = OssUtils.getOssPathToExportObject(taskName,
        EXPORT_VIEW_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_META_FILE_NAME);
    OssUtils.createFile(ossFileName, viewText);
  }
}
