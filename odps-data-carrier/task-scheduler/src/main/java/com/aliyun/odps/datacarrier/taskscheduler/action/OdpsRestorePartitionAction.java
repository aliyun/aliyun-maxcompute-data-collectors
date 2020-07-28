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
  private String destinationProject;
  private String tableName;
  private Map<String, String> settings;

  public OdpsRestorePartitionAction(String id, String taskName,
                                    String originProject,
                                    String destinationProject,
                                    String tableName,
                                    Map<String, String> settings) {
    super(id);
    this.taskName = taskName;
    this.originProject = originProject;
    this.destinationProject = destinationProject;
    this.tableName = tableName;
    this.settings = settings;
  }

  @Override
  String getSql() {
    try {
      String ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_TABLE_FOLDER,
          originProject,
          tableName,
          EXPORT_PARTITION_SPEC_FILE_NAME);
      String content = OssUtils.readFile(ossFileName);
      LOG.info("Meta file {}, content {}", ossFileName, content);
      StringBuilder builder = new StringBuilder();
      builder.append("ALTER TABLE\n");
      builder.append(destinationProject).append(".`").append(tableName).append("`\n");
      builder.append(content);
      String sql = builder.toString();
      LOG.info("Restore partitions for {} from {} to {} as {}", tableName, originProject, destinationProject, sql);
      return sql;
    } catch (Exception e) {
      LOG.error("Restore partitions for {} from {} to {} failed.", tableName, originProject, destinationProject, e);
    }
    return "";
  }

  @Override
  Map<String, String> getSettings() {
    return settings == null ? new HashMap<>() : settings;
  }
}
