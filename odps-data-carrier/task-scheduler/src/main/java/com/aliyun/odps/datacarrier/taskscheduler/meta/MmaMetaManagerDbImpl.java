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

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.MigrationJobInfo;
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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaExceptionFactory;
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
      List<MigrationJobInfo> jobInfos =
          MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, null, -1);

      for (MigrationJobInfo jobInfo : jobInfos) {
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

    try (Connection conn = ds.getConnection()) {
      try {

        MetaSource.TableMetaModel tableMetaModel =
            metaSource.getTableMetaWithoutPartitionMeta(db, tbl);
        boolean isPartitioned = tableMetaModel.partitionColumns.size() > 0;

        // Create or update mma table meta
        MigrationJobInfo jobInfo = new MigrationJobInfo(
            db,
            tbl,
            isPartitioned,
            config,
            MigrationStatus.PENDING,
            Constants.MMA_TBL_META_INIT_ATTEMPT_TIMES,
            Constants.MMA_TBL_META_INIT_LAST_SUCC_TIMESTAMP);

        MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);

        // Create or update mma partition meta
        if (isPartitioned) {
          MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, db);
          MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn, db, tbl);

          List<List<String>> newPartitionValuesList;
          if (config.getPartitionValuesList() != null) {
            // If partitions are specified, check their existence
            newPartitionValuesList = config.getPartitionValuesList();

            for (List<String> partitionValues : newPartitionValuesList) {
              if (!metaSource.hasPartition(db, tbl, partitionValues)) {
                // TODO: refine exception
                throw new MmaException("Partition not exists: " + partitionValues);
              }
            }
          } else {
            // If partitions not specified, get the latest metadata from metasouce
            newPartitionValuesList = metaSource.listPartitions(db, tbl);
          }

          newPartitionValuesList = MmaMetaManagerDbImplUtils.filterOutPartitions(
              conn, db, tbl, newPartitionValuesList);

          List<MigrationJobPtInfo> migrationJobPtInfos = newPartitionValuesList
              .stream()
              .map(ptv -> new MigrationJobPtInfo(ptv,
                                                 MigrationStatus.PENDING,
                                                 Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                                                 Constants.MMA_PT_META_INIT_LAST_SUCC_TIMESTAMP))
              .collect(Collectors.toList());
          MmaMetaManagerDbImplUtils.mergeIntoMmaPartitionMeta(conn, db, tbl, migrationJobPtInfos);
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
  public synchronized void removeMigrationJob(String db, String tbl) throws MmaException {

    LOG.info("Enter removeMigrationJob");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
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
  public synchronized List<TableMigrationConfig> listMigrationJobs(int limit)
      throws MmaException {

    return listMigrationJobsInternal(null, limit);
  }

  @Override
  public synchronized List<TableMigrationConfig> listMigrationJobs(
      MigrationStatus status,
      int limit)
      throws MmaException {

    return listMigrationJobsInternal(status, limit);
  }

  private List<TableMigrationConfig> listMigrationJobsInternal(
      MigrationStatus status,
      int limit)
      throws MmaException {

    try (Connection conn = ds.getConnection()) {
      try {
        List<MigrationJobInfo> jobInfos =
            MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, status, limit);
        List<TableMigrationConfig> migrationConfigs = new LinkedList<>();

        for (MigrationJobInfo jobInfo : jobInfos) {
          if (status == null) {
            migrationConfigs.add(jobInfo.getMigrationConfig());
          } else {
            if (jobInfo.isPartitioned()) {
              String db = jobInfo.getDb();
              String tbl = jobInfo.getTbl();
              MigrationStatus realStatus =
                  MmaMetaManagerDbImplUtils.inferPartitionedTableStatus(conn, db, tbl);
              if (status.equals(realStatus)) {
                migrationConfigs.add(jobInfo.getMigrationConfig());
              }
            } else {
              if (status.equals(jobInfo.getStatus())) {
                migrationConfigs.add(jobInfo.getMigrationConfig());
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
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }

        jobInfo.setStatus(status);
        // For a partitioned table, its migration status is inferred from its partitions' migration
        // statuses. And it does not have table level attr 'attemptTimes' or 'lastSuccTimestamp'.
        if (!jobInfo.isPartitioned()) {
          switch (status) {
            case SUCCEEDED: {
              jobInfo.setAttemptTimes(jobInfo.getAttemptTimes() + 1);
              jobInfo.setLastSuccTimestamp(System.currentTimeMillis());
              break;
            }
            case FAILED: {
              int attemptTimes = jobInfo.getAttemptTimes() + 1;
              int retryTimesLimit = jobInfo
                  .getMigrationConfig()
                  .getAdditionalTableConfig()
                  .getRetryTimesLimit();
              if (attemptTimes <= retryTimesLimit) {
                status = MigrationStatus.PENDING;
              }
              jobInfo.setStatus(status);
              jobInfo.setAttemptTimes(attemptTimes);
              jobInfo.setLastSuccTimestamp(Constants.MMA_PT_META_INIT_LAST_SUCC_TIMESTAMP);
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
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        int retryTimesLimit = jobInfo
            .getMigrationConfig()
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

          jobPtInfo.setStatus(status);
          switch (status) {
            case SUCCEEDED: {
              jobPtInfo.setAttemptTimes(jobPtInfo.getAttemptTimes() + 1);
              jobPtInfo.setLastSuccTimestamp(System.currentTimeMillis());
              break;
            }
            case FAILED: {
              int attemptTimes = jobPtInfo.getAttemptTimes() + 1;
              jobPtInfo.setStatus(status);
              if (attemptTimes <= retryTimesLimit) {
                jobPtInfo.setStatus(MigrationStatus.PENDING);
              }
              jobPtInfo.setAttemptTimes(attemptTimes);
              jobPtInfo.setLastSuccTimestamp(Constants.MMA_PT_META_INIT_LAST_SUCC_TIMESTAMP);
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
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
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
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
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
  public synchronized TableMigrationConfig getConfig(String db, String tbl)
      throws MmaException {
    LOG.info("Enter getConfig");

    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try (Connection conn = ds.getConnection()) {
      try {
        MigrationJobInfo jobInfo = MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, db, tbl);
        if (jobInfo == null) {
          throw MmaExceptionFactory.getMigrationJobNotExistedException(db, tbl);
        }
        return jobInfo.getMigrationConfig();
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
      List<MigrationJobInfo> jobInfos =
          MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, MigrationStatus.PENDING, -1);
      List<MetaSource.TableMetaModel> ret = new LinkedList<>();
      for (MigrationJobInfo jobInfo : jobInfos) {
        String db = jobInfo.getDb();
        String tbl = jobInfo.getTbl();

        MetaSource.TableMetaModel tableMetaModel;
        try {
          tableMetaModel = metaSource.getTableMetaWithoutPartitionMeta(db, tbl);
        } catch (Exception e) {
          // Table could be deleted after the task is submitted. In this case,
          // metaSource.getTableMetaWithoutPartitionMeta# will fail.
          LOG.warn("Failed to get metadata, db: {}, tbl: {}", db, tbl);
          updateStatusInternal(db, tbl, MigrationStatus.FAILED);
          // TODO: Should throw MMA meta exception here and stop the task scheduler
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
              LOG.warn("Failed to get metadata, db: {}, tbl: {}, pt: {}",
                       db, tbl, jobPtInfo.getPartitionValues());
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

        jobInfo.getMigrationConfig().apply(tableMetaModel);
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
