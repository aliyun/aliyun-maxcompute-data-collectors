package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource;

public interface MMAMetaManager {

  enum MigrationStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
  }

  /**
   * Init data migration of give table
   * @param db database name
   * @param tbl table name
   */
  void initMigration(String db, String tbl, TableMigrationConfig config);

  /**
   * Init data migration of given table
   * @param db database name
   * @param tbl table name
   * @param partitionValuesList list of partition values
   */
  void initMigration(String db,
                     String tbl,
                     List<List<String>> partitionValuesList,
                     TableMigrationConfig config);

  /**
   * Update migration status of given table
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


  TableMigrationConfig getConfig(String db, String tbl);

  int getFailedTimes(String db, String tbl);

  int getFailedTimes(String db, String tbl, List<String> partitionValues);

  void increaseFailedTimes(String db, String tbl);

  void increaseFailedTimes(String db, String tbl, List<String> partitionValues);

  List<MetaSource.TableMetaModel> getPendingTables();

  MetaSource.TableMetaModel getNextPendingTable();
}
