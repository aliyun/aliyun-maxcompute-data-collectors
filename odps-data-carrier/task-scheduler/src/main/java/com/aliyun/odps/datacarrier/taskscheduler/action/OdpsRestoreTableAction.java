package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_VIEW_FOLDER;

public class OdpsRestoreTableAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestoreTableAction.class);

  private String taskName;
  private String originProject;
  private String tableName;
  private String destinationProject;
  private String destinationTable;
  private MmaConfig.ObjectType type;
  private Map<String, String> settings;

  public OdpsRestoreTableAction(String id, String taskName,
                                String originProject,
                                String originTable,
                                String destinationProject,
                                String destinationTable,
                                MmaConfig.ObjectType type,
                                Map<String, String> settings) {
    super(id);
    this.taskName = taskName;
    this.originProject = originProject;
    this.destinationProject = destinationProject;
    this.type = type;
    this.tableName = originTable;
    this.destinationTable = destinationTable;
    this.settings = settings;
  }

  @Override
  String getSql() {
    try {
      String folder = MmaConfig.ObjectType.TABLE.equals(type) ? EXPORT_TABLE_FOLDER : EXPORT_VIEW_FOLDER;
      String ossFileName = OssUtils.getOssPathToExportObject(taskName,
          folder,
          originProject,
          tableName,
          EXPORT_META_FILE_NAME);
      String content = OssUtils.readFile(ossFileName);
      LOG.info("Meta file {}, content {}", ossFileName, content);
      StringBuilder builder = new StringBuilder();
      if (MmaConfig.ObjectType.VIEW.equals(type)) {
        builder.append(OdpsSqlUtils.getCreateViewStatement(destinationProject, tableName, content));
      } else {
        builder.append("CREATE EXTERNAL TABLE IF NOT EXISTS ")
            .append(destinationProject).append(".")
            .append("`").append(destinationTable).append("`")
            .append(content);
      }
      String sql = builder.toString();
      LOG.info("Restore {} from {}.{} to {}.{} as {}",
          type.name(), originProject, tableName, destinationProject,
          MmaConfig.ObjectType.VIEW.equals(type) ? tableName : destinationTable, sql);
      return sql;
    } catch (Exception e) {
      LOG.error("Restore {} {} from {}.{} to {}.{} failed.",
          type.name(), originProject, tableName, destinationProject,
          MmaConfig.ObjectType.VIEW.equals(type) ? tableName : destinationTable, e);
    }
    return "";
  }

  @Override
  Map<String, String> getSettings() {
    return settings == null ? new HashMap<>() : settings;
  }
}