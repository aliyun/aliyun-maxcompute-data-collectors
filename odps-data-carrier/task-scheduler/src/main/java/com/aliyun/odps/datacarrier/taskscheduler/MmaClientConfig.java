package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaClientConfig extends MmaServerConfig {
  private static final Logger LOG = LogManager.getLogger(MmaClientConfig.class);

  public MmaClientConfig(DataSource dataSource,
                         MmaConfig.OssConfig ossConfig,
                         MmaConfig.HiveConfig hiveConfig,
                         MmaConfig.OdpsConfig odpsConfig) {
    super(dataSource, ossConfig, hiveConfig, odpsConfig);
  }

  @Override
  public boolean validate() {
    boolean valid = true;

    switch (getDataSource()) {
      case Hive:
        if (getHiveConfig() == null || !getHiveConfig().validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", getHiveConfig());
        }
        break;
      case OSS:
        if (getOssConfig() == null || !getOssConfig().validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", getOssConfig());
        }
        break;
    }

    return valid;
  }

  public static MmaClientConfig fromFile(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    return GsonUtils.getFullConfigGson().fromJson(content, MmaClientConfig.class);
  }
}
