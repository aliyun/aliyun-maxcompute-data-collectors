package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;

public interface MMAMetaManager {

  enum MigrationStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
  }

  /**
   * Add a migration job of give table.
   *
   * @param config migration config
   */
  void addMigrationJob(MetaConfiguration.TableConfig config);

  /**
   * Remove migration job of given table.
   *
   * @param db database name
   * @param tbl table name
   */
  void removeMigrationJob(String db, String tbl);

  /**
   * Check if a migration job exists.
   *
   * @param db database name
   * @param tbl table name
   */
  boolean hasMigrationJob(String db, String tbl);

  /**
   * Update the status of a migration job. If the new status is FAILED, but the failed times
   * is less than the retry limitation, the status will be changed to PENDING instead.
   *
   * @param db database name
   * @param tbl table name
   * @param status migration status
   */
  void updateStatus(String db, String tbl, MigrationStatus status);

  /**
   * Update status of a migration job. If all of the partitions succeeded, the status will be
   * changed to SUCCEEDED automatically.
   * @param db database name
   * @param tbl table name
   * @param partitionValuesList list of partition values
   * @param status migration status
   */
  void updateStatus(String db,
                    String tbl,
                    List<List<String>> partitionValuesList,
                    MigrationStatus status);

  /**
   * Get status of a migration job.
   *
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(String db, String tbl);

  /**
   * Get migration status of specified partition.
   *
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(String db, String tbl, List<String> partitionValues);

  /**
   * Get config of a migration job.
   *
   * @param db database name
   * @param tbl table name
   * @return migration config
   */
  MetaConfiguration.TableConfig getConfig(String db, String tbl);

  /**
   * Get pending migration jobs.
   * @return
   */
  List<MetaSource.TableMetaModel> getPendingTables();


  /**
   * Get next pending migration job.
   * @return
   */
  MetaSource.TableMetaModel getNextPendingTable();
}
