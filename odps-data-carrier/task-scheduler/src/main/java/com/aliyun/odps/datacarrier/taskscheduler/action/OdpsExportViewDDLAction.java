package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_DDL_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_DDL_FOLDER_NAME;

public class OdpsExportViewDDLAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportViewDDLAction.class);

  private String viewText;

  public OdpsExportViewDDLAction(String id, String viewText) {
    super(id);
    this.viewText = viewText;
  }

  @Override
  public void doAction() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    String script = OdpsSqlUtils.getCreateViewStatement(tableMetaModel.databaseName, tableMetaModel.tableName, viewText);
    LOG.info("Task {}, script {}", id, script);
    String ossFileName = OssUtils.getOssPathToExportObject(EXPORT_TABLE_DDL_FOLDER_NAME,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_DDL_FILE_NAME);
    OssUtils.createFile(ossFileName, script);
  }
}
