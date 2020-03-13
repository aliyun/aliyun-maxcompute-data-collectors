package com.aliyun.odps.datacarrier.taskscheduler;

public interface MMAClient {
  public void createMigrationJobs(MetaConfiguration metaConfiguration);

  public MMAMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl);
}
