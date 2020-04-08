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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaClientFsImpl implements MmaClient {

  private static final Logger LOG = LogManager.getLogger(MmaClientFsImpl.class);

  private static final String ERROR_INDICATOR = "ERROR: ";
  private static final String WARNING_INDICATOR = "WARNING: ";

  private MetaSource metaSource;

  public MmaClientFsImpl(MmaClientConfig mmaClientConfig) throws MetaException, IOException {
    MmaConfig.HiveConfig hiveConfig = mmaClientConfig.getHiveConfig();
    metaSource = new HiveMetaSource(hiveConfig.getHmsThriftAddr(),
                                    hiveConfig.getKrbPrincipal(),
                                    hiveConfig.getKeyTab(),
                                    hiveConfig.getKrbSystemProperties());
    MmaMetaManagerFsImpl.init(null, metaSource);
  }

  @Override
  public void createMigrationJobs(MmaMigrationConfig mmaMigrationConfig) {
    // TODO: prevent user from creating too many migration jobs
    MmaConfig.AdditionalTableConfig globalAdditionalTableConfig =
        mmaMigrationConfig.getGlobalAdditionalTableConfig();

    if (mmaMigrationConfig.getServiceMigrationConfig() != null) {
      MmaConfig.ServiceMigrationConfig serviceMigrationConfig =
          mmaMigrationConfig.getServiceMigrationConfig();

      List<String> databases;
      try {
        databases = metaSource.listDatabases();
      } catch (Exception e) {
        String msg = "Failed to create migration jobs";
        System.err.println(ERROR_INDICATOR + msg);
        LOG.error(msg, e);
        return;
      }

      for (String database : databases) {
        createDatabaseMigrationJob(database,
                                   serviceMigrationConfig.getDestProjectName(),
                                   globalAdditionalTableConfig);
      }
    } else if (mmaMigrationConfig.getDatabaseMigrationConfigs() != null) {
      for (MmaConfig.DatabaseMigrationConfig databaseMigrationConfig :
          mmaMigrationConfig.getDatabaseMigrationConfigs()) {
        String database = databaseMigrationConfig.getSourceDatabaseName();

        if (!databaseExists(database)) {
          continue;
        }

        // TODO: merge additional table config
        // Use global additional table config if database migration config doesn't contain one
        MmaConfig.AdditionalTableConfig databaseAdditionalTableConfig =
            databaseMigrationConfig.getAdditionalTableConfig();
        if (databaseAdditionalTableConfig == null) {
          databaseAdditionalTableConfig = globalAdditionalTableConfig;
        }

        createDatabaseMigrationJob(database,
                                   databaseMigrationConfig.getDestProjectName(),
                                   databaseAdditionalTableConfig);
      }
    } else {
      for (MmaConfig.TableMigrationConfig tableMigrationConfig :
          mmaMigrationConfig.getTableMigrationConfigs()) {
        // TODO: merge additional table config
        if (tableMigrationConfig.getAdditionalTableConfig() == null) {
          tableMigrationConfig.setAdditionalTableConfig(
              mmaMigrationConfig.getGlobalAdditionalTableConfig());
        }

        String database = tableMigrationConfig.getSourceDataBaseName();
        String table = tableMigrationConfig.getSourceTableName();

        if (databaseExists(database) && tableExists(database, table)) {
          MmaMetaManagerFsImpl.getInstance().addMigrationJob(tableMigrationConfig);
          LOG.info("Job submitted, database: {}, table: {}",
                   tableMigrationConfig.getSourceDataBaseName(),
                   tableMigrationConfig.getSourceTableName());
        }
      }
    }
  }

  private boolean databaseExists(String database) {
    try {
      if (!metaSource.hasDatabase(database)) {
        String msg = "Database " + database + " not found";
        System.err.println(WARNING_INDICATOR + msg);
        LOG.warn(msg);
        return false;
      }
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for database:" + database;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return false;
    }
    return true;
  }

  private boolean tableExists(String database, String table) {
    try {
      if (!metaSource.hasTable(database, table)) {
        String msg = "Table " + database + "." + table + " not found";
        System.err.println("WARNING: " + msg);
        LOG.warn(msg);
        return false;
      }
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for table: " + database + "." + table;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return false;
    }
    return true;
  }

  private void createDatabaseMigrationJob(String database,
                                          String project,
                                          MmaConfig.AdditionalTableConfig databaseAdditionalTableConfig) {
    List<String> tables;
    try {
      tables = metaSource.listTables(database);
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for database:" + database;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return;
    }

    for (String table : tables) {
      MmaConfig.TableMigrationConfig tableMigrationConfig =
          new MmaConfig.TableMigrationConfig(
              database,
              table,
              project,
              table,
              databaseAdditionalTableConfig);

      MmaMetaManagerFsImpl.getInstance().addMigrationJob(tableMigrationConfig);
      LOG.info("Job submitted, database: {}, table: {}", database, table);
    }
  }

  @Override
  public void removeMigrationJob(String db, String tbl) {
    if (MmaMetaManagerFsImpl.getInstance().hasMigrationJob(db, tbl)) {
      MmaMetaManager.MigrationStatus status = MmaMetaManagerFsImpl.getInstance().getStatus(db, tbl);
      if (MmaMetaManager.MigrationStatus.PENDING.equals(status)) {
        String msg = String.format("Failed to remove migration job, database: %s, table: %s, "
                                   + "reason: status is RUNNING", db, tbl);
        LOG.error(msg);
        throw new IllegalArgumentException(ERROR_INDICATOR + msg);
      }
      MmaMetaManagerFsImpl.getInstance().removeMigrationJob(db, tbl);
    }

  }

  @Override
  public MmaMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl) {
    MmaMetaManager.MigrationStatus status =  MmaMetaManagerFsImpl.getInstance().getStatus(db, tbl);
    LOG.info("Get migration status, db: {}, tbl: {}, status: {}", db, tbl, status);

    return status;
  }

  @Override
  public MmaMetaManager.MigrationProgress getMigrationProgress(String db, String tbl) {
    MmaMetaManager.MigrationProgress progress =
        MmaMetaManagerFsImpl.getInstance().getProgress(db, tbl);
    LOG.info("Get migration progress, db: {}, tbl: {}, progress: {}",
             db, tbl, progress == null ? "N/A" : progress.toJson());

    return progress;
  }

  @Override
  public List<MmaConfig.TableMigrationConfig> listMigrationJobs(
      MmaMetaManager.MigrationStatus status) {

    List<MmaConfig.TableMigrationConfig> ret =
        MmaMetaManagerFsImpl.getInstance().listMigrationJobs(status, -1);
    LOG.info("Get migration job list, status: {}, ret: {}",
             status,
             ret.stream()
                 .map(c -> c.getSourceDataBaseName() + "." + c.getSourceTableName())
                 .collect(Collectors.joining(", ")));
    return ret;
  }
}
