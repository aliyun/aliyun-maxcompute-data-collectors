package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaServerConfig {
  private static final Logger LOG = LogManager.getLogger(MmaServerConfig.class);

  private DataSource dataSource;
  private MmaConfig.OssConfig ossConfig;
  private MmaConfig.HiveConfig hiveConfig;
  private MmaConfig.OdpsConfig odpsConfig;

  public MmaServerConfig(DataSource dataSource,
                         MmaConfig.OssConfig ossConfig,
                         MmaConfig.HiveConfig hiveConfig,
                         MmaConfig.OdpsConfig odpsConfig) {
    this.dataSource = dataSource;
    this.ossConfig = ossConfig;
    this.hiveConfig = hiveConfig;
    this.odpsConfig = odpsConfig;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public MmaConfig.OdpsConfig getOdpsConfig() {
    return odpsConfig;
  }

  public MmaConfig.HiveConfig getHiveConfig() {
    return hiveConfig;
  }

  public MmaConfig.OssConfig getOssConfig() {
    return ossConfig;
  }

  public String toJson() {
    return GsonUtils.getFullConfigGson().toJson(this);
  }

  public boolean validate() {
    boolean valid = true;

    switch (this.dataSource) {
      case Hive:
        if (!this.hiveConfig.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.hiveConfig);
        }
        break;
      case OSS:
        if (!this.ossConfig.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.ossConfig);
        }
        break;
    }

    if (!odpsConfig.validate()) {
      valid = false;
      LOG.error("Validate MetaConfiguration failed due to {}", this.odpsConfig);
    }

    return valid;
  }

  public static MmaServerConfig fromFile(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    return GsonUtils.getFullConfigGson().fromJson(content, MmaServerConfig.class);
  }
}
