package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_PARTITION_SPEC_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;

public class OdpsRestorePartitionAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestorePartitionAction.class);

  private String taskName;
  private String originProject;
  private String originTableName;
  private String destinationProject;
  private String destinationTableName;
  private Map<String, String> settings;

  public OdpsRestorePartitionAction(String id, String taskName,
                                    String originProject,
                                    String originTableName,
                                    String destinationProject,
                                    String destinationTableName,
                                    Map<String, String> settings) {
    super(id);
    this.taskName = taskName;
    this.originProject = originProject;
    this.originTableName = originTableName;
    this.destinationProject = destinationProject;
    this.destinationTableName = destinationTableName;
    this.settings = settings;
  }

  @Override
  String getSql() {
    try {
      String ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_TABLE_FOLDER,
          originProject,
          originTableName,
          EXPORT_PARTITION_SPEC_FILE_NAME);
      String content = OssUtils.readFile(ossFileName);
      LOG.info("Meta file {}, content {}", ossFileName, content);
      StringBuilder builder = new StringBuilder();
      builder.append("ALTER TABLE\n");
      builder.append(destinationProject).append(".`").append(destinationTableName).append("`\n");
      builder.append(content);
      String sql = builder.toString();
      LOG.info("Restore partitions from {}.{} to {}.{} as {}",
          originProject, originTableName, destinationProject, destinationTableName, sql);
      return sql;
    } catch (Exception e) {
      LOG.error("Restore partitions from {}.{} to {}.{} failed.",
          originProject, originTableName, destinationProject, destinationTableName, e);
    }
    return "";
  }

  @Override
  Map<String, String> getSettings() {
    return settings == null ? new HashMap<>() : settings;
  }
}
