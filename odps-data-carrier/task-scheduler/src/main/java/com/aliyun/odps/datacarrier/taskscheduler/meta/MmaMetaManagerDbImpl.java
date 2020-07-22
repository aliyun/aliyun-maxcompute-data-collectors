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

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.JobInfo;
import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.MigrationJobPtInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.AdditionalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.JobConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.JobType;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectType;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaExceptionFactory;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.PartitionMetaModel;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * This class implements {@link MmaMetaManager} using a H2 embedded database.
 */

public class MmaMetaManagerDbImpl implements MmaMetaManager {

  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerDbImpl.class);

  private HikariDataSource ds;
  private MetaSource metaSource;

  public MmaMetaManagerDbImpl(Path parentDir, MetaSource metaSource, boolean needRecover)
      throws MmaException {
    if (parentDir == null) {
      // Ensure MMA_HOME is set
      String mmaHome = System.getenv("MMA_HOME");
      if (mmaHome == null) {
        throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
      }
      parentDir = Paths.get(mmaHome);
    }

    this.metaSource = metaSource;

    LOG.info("Initialize MmaMetaManagerDbImpl");
    try {
      Class.forName("org.h2.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error("H2 JDBC driver not found");
      throw new IllegalStateException("Class not found: org.h2.Driver");
    }

    LOG.info("Create connection pool");
    String connectionUrl =
        "jdbc:h2:file:" + Paths.get(parentDir.toString(), Constants.DB_FILE_NAME).toAbsolutePath() +
        ";AUTO_SERVER=TRUE";
    setupDatasource(connectionUrl);
    LOG.info("JDBC connection URL: {}", connectionUrl);

    LOG.info("Create connection pool done");

    LOG.info("Setup database");
    try (Connection conn = ds.getConnection()) {
      MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);
      conn.commit();
    } catch (Throwable e) {
      throw new MmaException("Setting up database failed", e);
    }
    LOG.info("Setup database done");

    if (needRecover) {
      try {
        recover();
      } catch (Throwable e) {
        throw new IllegalStateException("Recover failed", e);
      }
    }
    LOG.info("Initialize MmaMetaManagerDbImpl done");
  }

  private void setupDatasource(String connectionUrl) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(connectionUrl);
    hikariConfig.setUsername("mma");
    hikariConfig.setPassword("mma");
    hikariConfig.setAutoCommit(false);
    hikariConfig.setMaximumPoolSize(50);
    hikariConfig.setMinimumIdle(1);
    hikariConfig.setTransactionIsolation("TRANSACTION_SERIALIZABLE");
    ds = new HikariDataSource(hikariConfig);
  }

  @Override
  public void shutdown() {
    LOG.info("Enter shutdown");
    metaSource.shutdown();
    ds.close();
    LOG.info("Leave shutdown");
  }

  private void recover() throws SQLException, MmaException {
    LOG.info("Enter recover");
    try (Connection conn = ds.getConnection()) {
      List<JobInfo> jobInfos =
          MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, null, -1);

      for (JobInfo jobInfo : jobInfos) {
        if (MigrationStatus.RUNNING.equals(jobInfo.getStatus())) {
          updateStatusInternal(jobInfo.getDb(), jobInfo.getTbl(), MigrationStatus.PENDING);
        }

        if (jobInfo.isPartitioned()) {
          List<MigrationJobPtInfo> jobPtInfos =
              MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                                   jobInfo.getDb(),
                                                                   jobInfo.getTbl(),
                                                                   MigrationStatus.RUNNING,
                                                                   -1);
          updateStatusInternal(jobInfo.getDb(),
                               jobInfo.getTbl(),
                               jobPtInfos
                                   .stream()
                                   .map(MigrationJobPtInfo::getPartitionValues)
                                   .collect(Collectors.toList()),
                               MigrationStatus.PENDING);
        }
      }

      conn.commit();
    }
    LOG.info("Leave recover");
  }

  @Override
  public synchronized void addMigrationJob(TableMigrationConfig config)
      throws MmaException {

    LOG.info("Enter addMigrationJob");

    if (config == null) {
      throw new IllegalArgumentException("'config' cannot be null");
    }

    String db = config.getSourceDataBaseName().toLowerCase();
    String tbl = config.getSourceTableName().toLowerCase();
    LOG.info("Add migration job, db: {}, tbl: {}", db, tbl);

    addMigrationJobInternal(
        db,
        tbl,
        config.getPartitionValuesList(),
        config.getAdditionalTableConfig(),
        TableMigrationConfig.toJson(config));
  }

  private void addMigrationJobInternal(
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      AdditionalTableConfig additionalTableConfig,
      String jsonizedConfig)
      throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo currentJobInfo =
            MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);

        if (currentJobInfo != null
            && MigrationStatus.RUNNING.equals(currentJobInfo.getStatus())) {
          throw MmaExceptionFactory.getRunningMigrationJobExistsException(db, tbl);
        }

        MetaSource.TableMetaModel tableMetaModel =
            metaSource.getTableMetaWithoutPartitionMeta(db, tbl);
        boolean isPartitioned = tableMetaModel.partitionColumns.size() > 0;

        JobConfig jobConfig = new JobConfig(
            db,
            tbl,
            JobType.MIGRATION,
            jsonizedConfig,
            additionalTableConfig);

        // Create or update mma table meta
        JobInfo jobInfo = new JobInfo(
            db,
            tbl,
            isPartitioned,
            jobConfig,
            MigrationStatus.PENDING,
            Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
            tableMetaModel.lastModifiedTime);

        MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);

        if (isPartitioned) {
          MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, db);
          MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn, db, tbl);

          List<MigrationJobPtInfo> jobPtInfosToMerge = new LinkedList<>();

          // Create or update mma partition meta
          // If partitions are specified, MMA will only create or update these partition. Else, MMA
          // will fetch all the partitions, then create meta for new partitions, reset meta for
          // failed partitions and modified partitions.
          // TODO: this behavior should be configurable
          if (partitionValuesList != null) {
            for (List<String> partitionValues : partitionValuesList) {
              // Check if user specified partitions exist
              if (!metaSource.hasPartition(db, tbl, partitionValues)) {
                throw new MmaException("Partition does not exist: " + partitionValues);
              } else {
                PartitionMetaModel partitionMetaModel =
                    metaSource.getPartitionMeta(db, tbl, partitionValues);
                jobPtInfosToMerge.add(new MigrationJobPtInfo(
                    partitionValues,
                    MigrationStatus.PENDING,
                    Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                    partitionMetaModel.lastModifiedTime));
              }
            }
          } else {
            List<MigrationJobPtInfo> jobPtInfos =
                MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn, db, tbl, null, -1);
            List<List<String>> totalPartitionValuesList = metaSource.listPartitions(db, tbl);

            for (List<String> partitionValues : totalPartitionValuesList) {
              MigrationJobPtInfo jobPtInfo = jobPtInfos
                  .stream()
                  .filter(info -> info.getPartitionValues().equals(partitionValues))
                  .findAny()
                  .orElse(null);
              PartitionMetaModel partitionMetaModel =
                  metaSource.getPartitionMeta(db, tbl, partitionValues);

              if (jobPtInfo == null) {
                LOG.info("Found new partition: {}", partitionValues);
              } else if (MigrationStatus.FAILED.equals(jobPtInfo.getStatus())) {
                LOG.info("Found failed partition: {}, resetting", partitionValues);
              } else if (MigrationStatus.SUCCEEDED.equals(jobPtInfo.getStatus())) {
                if (jobPtInfo.getLastModifiedTime() == null
                    || partitionMetaModel.lastModifiedTime == null) {
                  LOG.warn("Partition {} doesn't have last modified time, ignored",
                           partitionValues);
                  continue;
                } else if (partitionMetaModel.lastModifiedTime > jobPtInfo.getLastModifiedTime()) {
                  LOG.info("Found modified partition {}, old mtime: {}, new mtime: {}",
                           partitionValues,
                           jobPtInfo.getLastModifiedTime(),
                           partitionMetaModel.lastModifiedTime);
                }
              } else {
                // Ignore pending or running partitions
                continue;
              }

              jobPtInfosToMerge.add(new MigrationJobPtInfo(
                  partitionValues,
                  MigrationStatus.PENDING,
                  Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                  partitionMetaModel.lastModifiedTime));
            }
          }

          MmaMetaManagerDbImplUtils.mergeIntoMmaPartitionMeta(conn, db, tbl, jobPtInfosToMerge);
        }
        conn.commit();

        LOG.info("Leave addMigrationJob");
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Add migration job rollback failed, db: {}, tbl: {}", db, tbl);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToAddMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void addBackupJob(MmaConfig.ObjectExportConfig config) throws MmaException {
    String db = config.getDatabaseName().toLowerCase();
    String name = config.getObjectName().toLowerCase();

    LOG.info("Add backup job, db: {}, object name: {}, type: {}",
             db,
             name,
             config.getObjectType().name());

    try (Connection conn = ds.getConnection()) {
      JobInfo currentJobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, name);
      if (currentJobInfo != null
          && MigrationStatus.RUNNING.equals(currentJobInfo.getStatus())) {
          throw MmaExceptionFactory.getRunningMigrationJobExistsException(db, name);
      }

      try {
        if (ObjectType.TABLE.equals(config.getObjectType())) {
          // Convert table backup to a migration job
          addMigrationJobInternal(
              db,
              name,
              null,
              config.getAdditionalTableConfig(),
              MmaConfig.ObjectExportConfig.toJson(config));
        } else {
          JobConfig jobConfig = new JobConfig(
              db,
              name,
              JobType.BACKUP,
              MmaConfig.ObjectExportConfig.toJson(config),
              config.getAdditionalTableConfig());
          JobInfo jobInfo = new JobInfo(
              db,
              name,
              false,
              jobConfig,
              MigrationStatus.PENDING,
              Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
              Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);

          MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);

          conn.commit();
        }
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Add backup job rollback failed, db: {}, tbl: {}", db, name);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToAddMigrationJobException(db, name, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void removeMigrationJob(String db, String tbl) throws MmaException {

    LOG.info("Enter removeMigrationJob");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          return;
        } else {
          if (MigrationStatus.PENDING.equals(jobInfo.getStatus())) {
            // Restart running job is not allowed
            MmaException e = MmaExceptionFactory.getRunningMigrationJobExistsException(db, tbl);
            LOG.error(e);
            throw e;
          }
        }

        if (jobInfo.isPartitioned()) {
          MmaMetaManagerDbImplUtils.dropMmaPartitionMeta(conn, db, tbl);
        }
        MmaMetaManagerDbImplUtils.deleteFromMmaMeta(conn, db, tbl);

        conn.commit();
        LOG.info("Leave removeMigrationJob");
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Remove migration job rollback failed, db: {}, tbl: {}", db, tbl);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToRemoveMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized boolean hasMigrationJob(String db, String tbl) throws MmaException {
    LOG.info("Enter hasMigrationJob");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        return MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl) != null;
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized List<MmaConfig.JobConfig> listMigrationJobs(int limit)
      throws MmaException {

    return listMigrationJobsInternal(null, limit);
  }

  @Override
  public synchronized List<MmaConfig.JobConfig> listMigrationJobs(
      MigrationStatus status,
      int limit)
      throws MmaException {

    return listMigrationJobsInternal(status, limit);
  }

  private List<MmaConfig.JobConfig> listMigrationJobsInternal(
      MigrationStatus status,
      int limit)
      throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        List<JobInfo> jobInfos =
            MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, status, limit);
        List<MmaConfig.JobConfig> migrationConfigs = new LinkedList<>();

        for (JobInfo jobInfo : jobInfos) {
          if (status == null) {
            migrationConfigs.add(jobInfo.getJobConfig());
          } else {
            if (jobInfo.isPartitioned()) {
              String db = jobInfo.getDb();
              String tbl = jobInfo.getTbl();
              MigrationStatus realStatus =
                  MmaMetaManagerDbImplUtils.inferPartitionedTableStatus(conn, db, tbl);
              if (status.equals(realStatus)) {
                migrationConfigs.add(jobInfo.getJobConfig());
              }
            } else {
              if (status.equals(jobInfo.getStatus())) {
                migrationConfigs.add(jobInfo.getJobConfig());
              }
            }
          }
        }

        return migrationConfigs;
      } catch (Throwable e) {
        MmaException mmaException = MmaExceptionFactory.getFailedToListMigrationJobsException(e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void updateStatus(String db, String tbl, MigrationStatus status)
      throws MmaException {
    LOG.info("Enter updateStatus");

    if (db == null || tbl == null || status == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'status' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    updateStatusInternal(db, tbl, status);
    LOG.info("Leave updateStatus");
  }

  private void updateStatusInternal(String db, String tbl, MigrationStatus status)
      throws MmaException {
    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        TableMetaModel tableMetaModel = metaSource.getTableMeta(db, tbl);

        jobInfo.setStatus(status);
        jobInfo.setLastModifiedTime(tableMetaModel.lastModifiedTime);

        // For a partitioned table, its migration status is inferred from its partitions' migration
        // statuses. And it does not have table level attr 'attemptTimes'.
        if (!jobInfo.isPartitioned()) {
          switch (status) {
            case SUCCEEDED: {
              jobInfo.setAttemptTimes(jobInfo.getAttemptTimes() + 1);
              break;
            }
            case FAILED: {
              int attemptTimes = jobInfo.getAttemptTimes() + 1;
              int retryTimesLimit = jobInfo
                  .getJobConfig()
                  .getAdditionalTableConfig()
                  .getRetryTimesLimit();
              if (attemptTimes <= retryTimesLimit) {
                jobInfo.setStatus(MigrationStatus.PENDING);
              }
              jobInfo.setAttemptTimes(attemptTimes);
              break;
            }
            case RUNNING:
            case PENDING:
            default:
          }
        }
        MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);

        conn.commit();
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Update migration job rollback failed, db: {}, tbl: {}", db, tbl);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToUpdateMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized void updateStatus(
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MigrationStatus status)
      throws MmaException {
    LOG.info("Enter updateStatus");

    if (db == null || tbl == null || partitionValuesList == null || status == null) {
      throw new IllegalArgumentException(
          "'db' or 'tbl' or 'partitionValuesList' or 'status' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    updateStatusInternal(db, tbl, partitionValuesList, status);
    LOG.info("Leave updateStatus");
  }

  private void updateStatusInternal(
      String db,
      String tbl,
      List<List<String>> partitionValuesList,
      MigrationStatus status)
      throws MmaException {
    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        int retryTimesLimit = jobInfo
            .getJobConfig()
            .getAdditionalTableConfig()
            .getRetryTimesLimit();

        List<MigrationJobPtInfo> newJobPtInfos = new LinkedList<>();
        for (List<String> partitionValues : partitionValuesList) {
          MigrationJobPtInfo jobPtInfo =
              MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn, db, tbl, partitionValues);
          if (jobPtInfo == null) {
            throw MmaExceptionFactory
                .getMigrationJobPtNotExistedException(db, tbl, partitionValues);
          }
          PartitionMetaModel partitionMetaModel =
              metaSource.getPartitionMeta(db, tbl, partitionValues);

          jobPtInfo.setStatus(status);
          jobPtInfo.setLastModifiedTime(partitionMetaModel.lastModifiedTime);
          switch (status) {
            case SUCCEEDED: {
              jobPtInfo.setAttemptTimes(jobPtInfo.getAttemptTimes() + 1);
              break;
            }
            case FAILED: {
              int attemptTimes = jobPtInfo.getAttemptTimes() + 1;
              if (attemptTimes <= retryTimesLimit) {
                jobPtInfo.setStatus(MigrationStatus.PENDING);
              }
              jobPtInfo.setAttemptTimes(attemptTimes);
              LOG.info(GsonUtils.getFullConfigGson().toJson(jobPtInfo));
              break;
            }
            case RUNNING:
            case PENDING:
            default:
          }

          newJobPtInfos.add(jobPtInfo);
        }
        MmaMetaManagerDbImplUtils.mergeIntoMmaPartitionMeta(conn, db, tbl, newJobPtInfos);

        // Update the table level status
        MigrationStatus newStatus =
            MmaMetaManagerDbImplUtils.inferPartitionedTableStatus(conn, db, tbl);
        if (!jobInfo.getStatus().equals(newStatus)) {
          updateStatusInternal(db, tbl, newStatus);
        }

        conn.commit();
      } catch (Throwable e) {
        // Rollback
        if (conn != null) {
          try {
            conn.rollback();
          } catch (Throwable e2) {
            LOG.error("Update migration job pt rollback failed, db: {}, tbl: {}", db, tbl);
          }
        }

        MmaException mmaException =
            MmaExceptionFactory.getFailedToUpdateMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MigrationStatus getStatus(String db, String tbl) throws MmaException {
    LOG.info("Enter getStatus");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }

        if (jobInfo.isPartitioned()) {
          return MmaMetaManagerDbImplUtils.inferPartitionedTableStatus(conn,
                                                                       jobInfo.getDb(),
                                                                       jobInfo.getTbl());
        } else {
          return jobInfo.getStatus();
        }
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MigrationStatus getStatus(String db, String tbl, List<String> partitionValues)
      throws MmaException {
    LOG.info("Enter getStatus");

    if (db == null || tbl == null || partitionValues == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValues' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        MigrationJobPtInfo jobPtInfo =
            MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn, db, tbl, partitionValues);
        if (jobPtInfo == null) {
          throw MmaExceptionFactory.getMigrationJobPtNotExistedException(db, tbl, partitionValues);
        }
        return jobPtInfo.getStatus();
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobPtException(db, tbl, partitionValues);
        LOG.error(e);
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MigrationProgress getProgress(String db, String tbl) throws MmaException {
    LOG.info("Enter getProgress");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }

        if (!jobInfo.isPartitioned()) {
          return null;
        }

        Map<MigrationStatus, Integer> statusDistribution =
            MmaMetaManagerDbImplUtils.getPartitionStatusDistribution(conn, db, tbl);
        int pending = statusDistribution.getOrDefault(MigrationStatus.PENDING, 0);
        int running = statusDistribution.getOrDefault(MigrationStatus.RUNNING, 0);
        int succeeded = statusDistribution.getOrDefault(MigrationStatus.SUCCEEDED, 0);
        int failed = statusDistribution.getOrDefault(MigrationStatus.FAILED, 0);

        return new MigrationProgress(pending, running, succeeded, failed);
      } catch (Throwable e) {
        return null;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized MmaConfig.JobConfig getConfig(String db, String tbl)
      throws MmaException {
    LOG.info("Enter getConfig");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        JobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        return jobInfo.getJobConfig();
      } catch (Throwable e) {
        MmaException mmaException =
            MmaExceptionFactory.getFailedToGetMigrationJobException(db, tbl, e);
        LOG.error(ExceptionUtils.getStackTrace(e));
        throw mmaException;
      }
    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    }
  }

  @Override
  public synchronized List<MetaSource.TableMetaModel> getPendingTables() throws MmaException {
    LOG.info("Enter getPendingTables");

    try (Connection conn = ds.getConnection()) {
      List<JobInfo> jobInfos =
          MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, MigrationStatus.PENDING, -1);
      List<MetaSource.TableMetaModel> ret = new LinkedList<>();
      for (JobInfo jobInfo : jobInfos) {
        String db = jobInfo.getDb();
        String tbl = jobInfo.getTbl();

        MetaSource.TableMetaModel tableMetaModel;
        try {
          if (MmaConfig.JobType.BACKUP.equals(jobInfo.getJobConfig().getJobType())) {
            MmaConfig.ObjectExportConfig config =
                MmaConfig.ObjectExportConfig.fromJson(jobInfo.getJobConfig().getDescription());
            if (!MmaConfig.ObjectType.TABLE.equals(config.getObjectType())) {
              tableMetaModel = new MetaSource.TableMetaModel();
              tableMetaModel.databaseName = config.getDatabaseName();
              tableMetaModel.tableName = config.getObjectName();
              ret.add(tableMetaModel);
              continue;
            }
          }
          tableMetaModel = metaSource.getTableMetaWithoutPartitionMeta(db, tbl);
        } catch (Exception e) {
          // Table could be deleted after the task is submitted. In this case,
          // metaSource.getTableMetaWithoutPartitionMeta# will fail.
          LOG.warn("Failed to get metadata, db: {}, tbl: {}, stacktrace: {}",
                   db, tbl, ExceptionUtils.getStackTrace(e));
          updateStatusInternal(db, tbl, MigrationStatus.FAILED);
          continue;
        }

        if (jobInfo.isPartitioned()) {
          List<MigrationJobPtInfo> jobPtInfos =
              MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                                   db,
                                                                   tbl,
                                                                   MigrationStatus.PENDING,
                                                                   -1);

          List<MetaSource.PartitionMetaModel> partitionMetaModels = new LinkedList<>();
          for (MigrationJobPtInfo jobPtInfo : jobPtInfos) {
            try {
              partitionMetaModels.add(
                  metaSource.getPartitionMeta(db, tbl, jobPtInfo.getPartitionValues()));
            } catch (Exception e) {
              // Partitions could be deleted after the task is submitted. In this case,
              // metaSource.getPartitionMeta# will fail.
              LOG.warn("Failed to get metadata, db: {}, tbl: {}, pt: {}, stacktrace: {}",
                       db, tbl, jobPtInfo.getPartitionValues(), ExceptionUtils.getStackTrace(e));
              updateStatusInternal(
                  db,
                  tbl,
                  Collections.singletonList(jobPtInfo.getPartitionValues()),
                  MigrationStatus.FAILED);
              // TODO: Should throw MMA meta exception here and stop the task scheduler
            }
          }
          tableMetaModel.partitions = partitionMetaModels;
        }

        if (MmaConfig.JobType.MIGRATION.equals(jobInfo.getJobConfig().getJobType())) {
          TableMigrationConfig tableMigrationConfig =
              TableMigrationConfig.fromJson(jobInfo.getJobConfig().getDescription());
          tableMigrationConfig.apply(tableMetaModel);
        } else if (MmaConfig.JobType.BACKUP.equals(jobInfo.getJobConfig().getJobType())) {
          MmaConfig.ObjectExportConfig objectExportConfig =
              MmaConfig.ObjectExportConfig.fromJson(jobInfo.getJobConfig().getDescription());
          objectExportConfig.apply(tableMetaModel);
        }
        ret.add(tableMetaModel);
      }

      // Sort by name, make it easy to test
      ret.sort(Comparator.comparing(a -> (a.databaseName + a.tableName)));
      return ret;

    } catch (SQLException e) {
      throw MmaExceptionFactory.getFailedToCreateConnectionException(e);
    } catch (Throwable e) {
      MmaException mmaException = MmaExceptionFactory.getFailedToGetPendingJobsException(e);
      LOG.error(e);
      throw mmaException;
    }
  }

  @Override
  public synchronized MetaSource.TableMetaModel getNextPendingTable() {
    throw new UnsupportedOperationException();
  }
}
