package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;

public interface MmaClient {

  public void createMigrationJobs(MmaMigrationConfig mmaMigrationConfig);

  public List<MmaConfig.TableMigrationConfig> listMigrationJobs(MmaMetaManager.MigrationStatus status);

  public void removeMigrationJob(String db, String tbl);

  public MmaMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl);

  public MmaMetaManager.MigrationProgress getMigrationProgress(String db, String tbl);
}
