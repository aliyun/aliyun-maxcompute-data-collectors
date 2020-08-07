/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.DatabaseRestoreConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectExportConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectRestoreConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.*;

public interface MmaMetaManager {

  enum MigrationStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
  }

  class MigrationProgress {
    private int numPendingPartitions;
    private int numRunningPartitions;
    private int numSucceededPartitions;
    private int numFailedPartitions;

    public MigrationProgress(int numPendingPartitions,
                             int numRunningPartitions,
                             int numSucceededPartitions,
                             int numFailedPartitions) {
      this.numPendingPartitions = numPendingPartitions;
      this.numRunningPartitions = numRunningPartitions;
      this.numSucceededPartitions = numSucceededPartitions;
      this.numFailedPartitions = numFailedPartitions;
    }

    public int getNumPendingPartitions() {
      return numPendingPartitions;
    }

    public int getNumRunningPartitions() {
      return numRunningPartitions;
    }

    public int getNumSucceededPartitions() {
      return numSucceededPartitions;
    }

    public int getNumFailedPartitions() {
      return numFailedPartitions;
    }

    public String toJson() {
      return GsonUtils.getFullConfigGson().toJson(this);
    }
  }

  /**
   * Add a migration job of give table.
   *
   * TODO: explain the behavior when migration job exists in detail
   *
   * @param config migration config
   */
  void addMigrationJob(TableMigrationConfig config) throws MmaException;

  /**
   * Add a backup job of give table.
   * @param config backup config
   * @throws MmaException
   */
  void addBackupJob(ObjectExportConfig config) throws MmaException;

  void addObjectRestoreJob(ObjectRestoreConfig config) throws MmaException;

  void addDatabaseRestoreJob(DatabaseRestoreConfig config) throws MmaException;

  void mergeJobInfoIntoRestoreDB(RestoreTaskInfo taskInfo) throws MmaException;

  void updateStatusInRestoreDB(RestoreTaskInfo taskInfo,
                               MigrationStatus newStatus) throws MmaException;

  /**
   * Remove migration job of given table.
   *
   * @param db database name
   * @param tbl table name
   */
  void removeMigrationJob(String db, String tbl) throws MmaException;

  /**
   * Check if a migration job exists.
   *
   * @param db database name
   * @param tbl table name
   */
  boolean hasMigrationJob(String db, String tbl) throws MmaException;

  JobInfo getMigrationJob(String db, String tbl) throws MmaException;

  /**
   * List migration jobs
   * @return all migration jobs
   */
  List<MmaConfig.JobConfig> listMigrationJobs(int limit) throws MmaException;

  /**
   * List migration jobs in given status
   * @param status migration status
   * @return migration jobs in given status
   */
  List<MmaConfig.JobConfig> listMigrationJobs(
      MigrationStatus status,
      int limit) throws MmaException;

  List<RestoreTaskInfo> listRestoreJobs(String condition, int limit) throws MmaException;

  void removeRestoreJob(String uniqueId) throws MmaException;

  /**
   * Update the status of a migration job. If the new status is FAILED, but the failed times
   * is less than the retry limitation, the status will be changed to PENDING instead.
   *
   * @param db database name
   * @param tbl table name
   * @param status migration status
   */
  void updateStatus(String db, String tbl, MigrationStatus status) throws MmaException;

  /**
   * Update status of a migration job. If all of the partitions succeeded, the status will be
   * changed to SUCCEEDED automatically.
   * @param db database name
   * @param tbl table name
   * @param partitionValuesList list of partition values
   * @param status migration status
   */
  void updateStatus(
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MigrationStatus status)
      throws MmaException;

  /**
   * Get status of a migration job.
   *
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(String db, String tbl) throws MmaException;

  /**
   * Get migration status of specified partition.
   *
   * @param db database name
   * @param tbl table name
   * @return migration status
   */
  MigrationStatus getStatus(
      String db,
      String tbl,
      List<String> partitionValues)
      throws MmaException;

  /**
   * Get migration progress.
   * @param db database name
   * @param tbl table name
   * @return for partitioned tables, a {@link MigrationProgress} object will be returned. For
   * non-partitioned tables, null will be returned.
   */
  MigrationProgress getProgress(String db, String tbl) throws MmaException;

  /**
   * Get config of a migration job.
   *
   * @param db database name
   * @param tbl table name
   * @return migration config
   */
  MmaConfig.JobConfig getConfig(String db, String tbl) throws MmaException;

  /**
   * Get pending migration jobs.
   * @return
   */
  List<MetaSource.TableMetaModel> getPendingTables() throws MmaException;

  /**
   * Get next pending migration job.
   * @return
   */
  MetaSource.TableMetaModel getNextPendingTable() throws MmaException;

  void shutdown() throws MmaException;
}
