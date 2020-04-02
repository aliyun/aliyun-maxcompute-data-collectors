package com.aliyun.odps.datacarrier.taskscheduler;

    import java.util.List;

public interface MmaClient {

  public void createMigrationJobs(MmaMigrationConfig mmaMigrationConfig) throws MmaException;

  public List<MmaConfig.TableMigrationConfig> listMigrationJobs(
      MmaMetaManager.MigrationStatus status)
      throws MmaException;

  public void removeMigrationJob(String db, String tbl) throws MmaException;

  public MmaMetaManager.MigrationStatus getMigrationJobStatus(
      String db,
      String tbl)
      throws MmaException;

  public MmaMetaManager.MigrationProgress getMigrationProgress(
      String db,
      String tbl)
      throws MmaException;
}
