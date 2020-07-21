package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_DDL_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_DDL_FOLDER_NAME;

public class OdpsExportTableDDLAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportTableDDLAction.class);

  public OdpsExportTableDDLAction(String id) {
    super(id);
    ((OdpsSqlActionInfo) actionInfo).setResultType(OdpsSqlActionInfo.ResultType.RAW_DATA);
  }

  @Override
  String getSql() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    try {
      return OdpsSqlUtils.getDDLSql(tableMetaModel);
    } catch (Exception e) {
      LOG.error("Exception when get ddl sql for {}.{}", tableMetaModel.databaseName, tableMetaModel.tableName, e);
    }
    return null;
  }

  @Override
  Map<String, String> getSettings() {
    return new HashMap<>();
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = (List<List<String>>) future.get();
      if (rows.size() != 1 || rows.get(0).size() != 1) {
        LOG.error("Task {}, unexpected result {}", id, rows);
        setProgress(ActionProgress.FAILED);
        return;
      }
      MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
      String query = rows.get(0).get(0);
      if (!tableMetaModel.partitionColumns.isEmpty()) {
        tableMetaModel.odpsProjectName = tableMetaModel.databaseName;
        tableMetaModel.odpsTableName = tableMetaModel.tableName;
        query += OdpsSqlUtils.getAddPartitionStatement(tableMetaModel, Integer.MAX_VALUE, null);
      }
      LOG.info("Task {}, content {}", id, query);
      String ossFileName = OssUtils.getOssPathToExportObject(EXPORT_TABLE_DDL_FOLDER_NAME,
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          EXPORT_DDL_FILE_NAME);
      OssUtils.createFile(ossFileName, query);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id, ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }
}
