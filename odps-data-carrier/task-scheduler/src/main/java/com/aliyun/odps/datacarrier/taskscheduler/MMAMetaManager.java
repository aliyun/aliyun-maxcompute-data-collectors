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
   * Init data migration of give table
   */
  void initMigration(MetaConfiguration.TableConfig config);

  /**
   * Update migration status of given table. If the status is FAILED, but the failed times is less
   * than the retry limitation, the status will be changed to PENDING.
   * @param db database name
   * @param tbl table name
   * @param status migration status
   */
  void updateStatus(String db, String tbl, MigrationStatus status);

  /**
   * Update migration status of given table
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
   * Get migration status of given table
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(String db, String tbl);

  /**
   * Get migration status of given table
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(String db, String tbl, List<String> partitionValues);

  MetaConfiguration.TableConfig getConfig(String db, String tbl);

  List<MetaSource.TableMetaModel> getPendingTables();

  MetaSource.TableMetaModel getNextPendingTable();
}
