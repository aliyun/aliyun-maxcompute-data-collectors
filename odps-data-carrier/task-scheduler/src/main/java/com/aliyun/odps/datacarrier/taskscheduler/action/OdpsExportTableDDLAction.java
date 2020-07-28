package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_PARTITION_SPEC_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;

public class OdpsExportTableDDLAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportTableDDLAction.class);

  private String taskName;

  public OdpsExportTableDDLAction(String id, String taskName) {
    super(id);
    this.taskName = taskName;
  }

  @Override
  public void doAction() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    String location = OssUtils.getOssPathToExportObject(
            taskName,
            Constants.EXPORT_TABLE_FOLDER,
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            Constants.EXPORT_TABLE_DATA_FOLDER);
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        OdpsSqlUtils.getOssTablePath(ossConfig.getOssEndpoint(), ossConfig.getOssBucket(), location));
    String statement = OdpsSqlUtils.getCreateTableStatementWithoutDatabaseName(
        tableMetaModel, ossExternalTableConfig);
    LOG.info("Action {}, Task {}, export table {}.{}, statement {}",
        id, taskName, tableMetaModel.databaseName, tableMetaModel.tableName, statement);
    String ossFileName = OssUtils.getOssPathToExportObject(taskName,
        EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_META_FILE_NAME);
    OssUtils.createFile(ossFileName, statement);
    if (!tableMetaModel.partitionColumns.isEmpty()) {
      String addPartitionStatement = OdpsSqlUtils.getAddPartitionStatementWithoutDatabaseName(tableMetaModel);
      ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_TABLE_FOLDER,
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          EXPORT_PARTITION_SPEC_FILE_NAME);
      OssUtils.createFile(ossFileName, addPartitionStatement);
      LOG.info("Action {}, Task {}, export partition spec {}.{}, statement {}",
        id, taskName, tableMetaModel.databaseName, tableMetaModel.tableName, addPartitionStatement);
    }
  }
}
