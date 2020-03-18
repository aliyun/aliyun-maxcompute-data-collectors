package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;

public class MmaMigrationConfig implements MmaConfig.Config {
  private String user;
  private MmaConfig.AdditionalTableConfig globalAdditionalTableConfig;
  private MmaConfig.ServiceMigrationConfig serviceMigrationConfig;
  private List<MmaConfig.DatabaseMigrationConfig> databaseMigrationConfigs;
  private List<MmaConfig.TableMigrationConfig> tableMigrationConfigs;

  public MmaMigrationConfig(String user,
                            List<MmaConfig.TableMigrationConfig> tableMigrationConfigs,
                            MmaConfig.AdditionalTableConfig globalAdditionalTableConfig) {
    this.user = user;
    this.serviceMigrationConfig = serviceMigrationConfig;
    this.databaseMigrationConfigs = databaseMigrationConfigs;
    this.tableMigrationConfigs = tableMigrationConfigs;
    this.globalAdditionalTableConfig = globalAdditionalTableConfig;
  }

  @Override
  public boolean validate() {
    boolean valid;

    if (serviceMigrationConfig != null) {
      if (databaseMigrationConfigs != null && !databaseMigrationConfigs.isEmpty()
          || tableMigrationConfigs != null && !tableMigrationConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "Service migration config exists, please remove database and table migration configs");
      }
      valid = serviceMigrationConfig.validate();
    } else if (databaseMigrationConfigs != null) {
      if (tableMigrationConfigs != null && !tableMigrationConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "Service migration config exists, please remove table migration configs");
      }

      valid = databaseMigrationConfigs.stream()
          .allMatch(MmaConfig.DatabaseMigrationConfig::validate);
    } else {
      if (tableMigrationConfigs == null) {
        throw new IllegalArgumentException("No migration config found");
      }

      valid = tableMigrationConfigs.stream().allMatch(MmaConfig.TableMigrationConfig::validate);
    }

    return valid && (globalAdditionalTableConfig == null || globalAdditionalTableConfig.validate());
  }

  public String getUser() {
    return user;
  }

  public MmaConfig.ServiceMigrationConfig getServiceMigrationConfig() {
    return serviceMigrationConfig;
  }

  public List<MmaConfig.DatabaseMigrationConfig> getDatabaseMigrationConfigs() {
    return databaseMigrationConfigs;
  }

  public List<MmaConfig.TableMigrationConfig> getTableMigrationConfigs() {
    return tableMigrationConfigs;
  }

  public MmaConfig.AdditionalTableConfig getGlobalAdditionalTableConfig() {
    return globalAdditionalTableConfig;
  }

  public String toJson() {
    return GsonUtils.getFullConfigGson().toJson(this);
  }

  public static MmaMigrationConfig fromFile(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    return GsonUtils.getFullConfigGson().fromJson(content, MmaMigrationConfig.class);
  }
}
