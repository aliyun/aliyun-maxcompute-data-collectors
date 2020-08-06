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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;

public class MmaMetaManagerDbImplUtilsTest {
  private static final Path PARENT_DIR = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
  private static final String CONN_URL =
      "jdbc:h2:file:" + Paths.get(PARENT_DIR.toString(), "MmaMetaManagerDbImplUtilsTest")
      + ";AUTO_SERVER=TRUE";

  private static Connection conn;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    conn = DriverManager.getConnection(CONN_URL, "mma", "");
  }

  @AfterClass
  public static void afterClass() throws MmaException, SQLException {
    conn.close();
    File[] dbFiles = PARENT_DIR
        .toFile()
        .listFiles(f -> f.getName().startsWith("MmaMetaManagerDbImplUtilsTest"));

    assert dbFiles != null;
    for (File f : dbFiles) {
      assert f.delete();
    }
  }

  @Before
  public void setUp() throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + Constants.MMA_TBL_META_TBL_NAME);
    }

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SHOW SCHEMAS")) {
        while (!rs.isClosed() && rs.next()) {
          if (rs.getString(1).startsWith(Constants.MMA_PT_META_SCHEMA_NAME_PREFIX)) {
            stmt.execute("DROP SCHEMA IF EXISTS " + rs.getString(1) + " CASCADE");
          }
        }
      }
    }
  }

  @Test
  public void testCreateMmaTableMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
        boolean created = false;
        while (rs.next()) {
          if (Constants.MMA_TBL_META_TBL_NAME.equalsIgnoreCase(rs.getString(1))) {
            created = true;
          } else {
            Assert.fail("Unexpected table: " + rs.getString(1));
          }
        }

        Assert.assertTrue(created);
      }
    }
  }

  @Test
  public void testCreateMmaPartitionMetaSchema() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    String expectedSchemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                              MockHiveMetaSource.DB_NAME);
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SHOW SCHEMAS")) {
        boolean created = false;
        while (rs.next()) {
          if (expectedSchemaName.equalsIgnoreCase(rs.getString(1))) {
            created = true;
          } else if (!"PUBLIC".equalsIgnoreCase(rs.getString(1)) &&
                     !"INFORMATION_SCHEMA".equalsIgnoreCase(rs.getString(1))) {
            Assert.fail("Unexpected schema: " + rs.getString(1));
          }
        }

        Assert.assertTrue(created);
      }
    }
  }

  @Test
  public void testCreateMmaPartitionMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);
    String expectedSchemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                              MockHiveMetaSource.DB_NAME);
    String expectedTableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                             MockHiveMetaSource.TBL_PARTITIONED);

    try (Statement stmt = conn.createStatement()) {
      String sql = "SHOW TABLES FROM " + expectedSchemaName;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        boolean created = false;
        while (rs.next()) {
          if (expectedTableName.equalsIgnoreCase(rs.getString(1))) {
            created = true;
          } else {
            Assert.fail("Unexpected table: " + rs.getString(1));
          }
        }

        Assert.assertTrue(created);
      }
    }
  }

  @Test
  public void testMergeIntoMmaTableMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);
    MmaMetaManagerDbImplUtils.JobInfo jobInfo =
        new MmaMetaManagerDbImplUtils.JobInfo(
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED,
            true,
            MmaMetaManagerDbImplTest.PARTITIONED_TABLE_MIGRATION_JOB_CONFIG,
            MmaMetaManager.MigrationStatus.PENDING,
            Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
            Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);

    MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);

    try (Statement stmt = conn.createStatement()) {
      String sql = "SELECT * FROM " + Constants.MMA_TBL_META_TBL_NAME;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          Assert.assertEquals(MockHiveMetaSource.DB_NAME, rs.getString(1));
          Assert.assertEquals(MockHiveMetaSource.TBL_PARTITIONED, rs.getString(2));
          Assert.assertTrue(rs.getBoolean(3));
          Assert.assertEquals(GsonUtils.getFullConfigGson().toJson(MmaMetaManagerDbImplTest.TABLE_MIGRATION_CONFIG_PARTITIONED),
                              rs.getString(4));
          Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING.toString(),
                              rs.getString(5));
          Assert.assertEquals(Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                              rs.getInt(6));
          Assert.assertEquals(Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME,
                              rs.getLong(7));
          rowCount += 1;
        }

        Assert.assertEquals(1, rowCount);
      }
    }
  }

  @Test
  public void testDeleteFromMmaMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);
    MmaMetaManagerDbImplUtils.JobInfo jobInfo =
        new MmaMetaManagerDbImplUtils.JobInfo(
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED,
            true,
            MmaMetaManagerDbImplTest.PARTITIONED_TABLE_MIGRATION_JOB_CONFIG,
            MmaMetaManager.MigrationStatus.PENDING,
            Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
            Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);

    MmaMetaManagerDbImplUtils.mergeIntoMmaTableMeta(conn, jobInfo);
    MmaMetaManagerDbImplUtils.deleteFromMmaMeta(conn, MockHiveMetaSource.DB_NAME,
                                                MockHiveMetaSource.TBL_PARTITIONED);

    try (Statement stmt = conn.createStatement()) {
      String sql = "SELECT * FROM " + Constants.MMA_TBL_META_TBL_NAME;
      try (ResultSet rs = stmt.executeQuery(sql)) {
        Assert.assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testSelectSingleRecordFromMmaTableMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);

    String migrationConfigJson = GsonUtils.getFullConfigGson().toJson(
        MmaMetaManagerDbImplTest.PARTITIONED_TABLE_MIGRATION_JOB_CONFIG);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s VALUES('%s', '%s', %b, '%s', '%s', %d, %d)",
                                 Constants.MMA_TBL_META_TBL_NAME,
                                 MockHiveMetaSource.DB_NAME,
                                 MockHiveMetaSource.TBL_PARTITIONED,
                                 true,
                                 migrationConfigJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                                 Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    MmaMetaManagerDbImplUtils.JobInfo migrationJobInfo =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(
            conn,
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED);

    Assert.assertNotNull(migrationJobInfo);
    Assert.assertEquals(MockHiveMetaSource.DB_NAME, migrationJobInfo.getDb());
    Assert.assertEquals(MockHiveMetaSource.TBL_PARTITIONED, migrationJobInfo.getTbl());
    Assert.assertTrue(migrationJobInfo.isPartitioned());
    Assert.assertEquals(migrationConfigJson,
                        GsonUtils.getFullConfigGson().toJson(migrationJobInfo.getJobConfig()));
    Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING, migrationJobInfo.getStatus());
    Assert.assertEquals(Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                        migrationJobInfo.getAttemptTimes());
    Assert.assertEquals(Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME,
                        migrationJobInfo.getLastModifiedTime());
  }

  @Test
  public void testSelectSingleRecordFromMmaTableMetaNonExisted() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);


    MmaMetaManagerDbImplUtils.JobInfo migrationJobInfo =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(
            conn,
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED);

    Assert.assertNull(migrationJobInfo);
  }

  @Test
  public void testSelectRecordsFromMmaTableMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);

    String migrationConfigJson = GsonUtils.getFullConfigGson().toJson(
        MmaMetaManagerDbImplTest.PARTITIONED_TABLE_MIGRATION_JOB_CONFIG);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s VALUES('%s', '%s', %b, '%s', '%s', %d, %d)",
                                 Constants.MMA_TBL_META_TBL_NAME,
                                 MockHiveMetaSource.DB_NAME,
                                 MockHiveMetaSource.TBL_PARTITIONED,
                                 true,
                                 migrationConfigJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                                 Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    List<MmaMetaManagerDbImplUtils.JobInfo> migrationJobInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, null,  -1);

    Assert.assertEquals(1, migrationJobInfos.size());
    MmaMetaManagerDbImplUtils.JobInfo migrationJobInfo = migrationJobInfos.get(0);

    Assert.assertNotNull(migrationJobInfo);
    Assert.assertEquals(MockHiveMetaSource.DB_NAME, migrationJobInfo.getDb());
    Assert.assertEquals(MockHiveMetaSource.TBL_PARTITIONED, migrationJobInfo.getTbl());
    Assert.assertTrue(migrationJobInfo.isPartitioned());
    Assert.assertEquals(migrationConfigJson,
                        GsonUtils.getFullConfigGson().toJson(migrationJobInfo.getJobConfig()));
    Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING, migrationJobInfo.getStatus());
    Assert.assertEquals(Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                        migrationJobInfo.getAttemptTimes());
    Assert.assertEquals(Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME,
                        migrationJobInfo.getLastModifiedTime());
  }

  @Test
  public void testSelectRecordsFromMmaTableMetaEmpty() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);

    List<MmaMetaManagerDbImplUtils.JobInfo> migrationJobInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, null,  -1);

    Assert.assertEquals(0, migrationJobInfos.size());
  }

  @Test
  public void testSelectRecordsFromMmaTableMetaWithStatus() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);

    String migrationConfigJson = GsonUtils.getFullConfigGson().toJson(
        MmaMetaManagerDbImplTest.TABLE_MIGRATION_CONFIG_PARTITIONED);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s VALUES('%s', '%s', %b, '%s', '%s', %d, %d)",
                                 Constants.MMA_TBL_META_TBL_NAME,
                                 MockHiveMetaSource.DB_NAME,
                                 MockHiveMetaSource.TBL_PARTITIONED,
                                 true,
                                 migrationConfigJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                                 Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    List<MmaMetaManagerDbImplUtils.JobInfo> migrationJobInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn,
                                                         MmaMetaManager.MigrationStatus.SUCCEEDED,
                                                         -1);

    Assert.assertEquals(0, migrationJobInfos.size());
  }

  @Test
  public void testSelectRecordsFromMmaTableMetaWithLimit() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaTableMeta(conn);

    String migrationConfigJson = GsonUtils.getFullConfigGson().toJson(
        MmaMetaManagerDbImplTest.TABLE_MIGRATION_CONFIG_PARTITIONED);
    try (Statement stmt = conn.createStatement()) {
      for (int i = 0; i < 10; i++) {
        String dml = String.format("INSERT INTO %s VALUES('%s', '%s', %b, '%s', '%s', %d, %d)",
                                   Constants.MMA_TBL_META_TBL_NAME,
                                   MockHiveMetaSource.DB_NAME,
                                   MockHiveMetaSource.TBL_PARTITIONED + i,
                                   true,
                                   migrationConfigJson,
                                   MmaMetaManager.MigrationStatus.PENDING.toString(),
                                   Constants.MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES,
                                   Constants.MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME);
        stmt.execute(dml);
      }
    }


    List<MmaMetaManagerDbImplUtils.JobInfo> migrationJobInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaTableMeta(conn, null, 1);

    Assert.assertEquals(1, migrationJobInfos.size());
  }

  @Test
  public void testMergeIntoMmaPartitionMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn,
                                                           MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    MmaMetaManagerDbImplUtils.MigrationJobPtInfo jobPtInfo =
        new MmaMetaManagerDbImplUtils.MigrationJobPtInfo(
            MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES,
            MmaMetaManager.MigrationStatus.PENDING,
            Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
            Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME);
    MmaMetaManagerDbImplUtils.mergeIntoMmaPartitionMeta(conn,
                                                        MockHiveMetaSource.DB_NAME,
                                                        MockHiveMetaSource.TBL_PARTITIONED,
                                                        Collections.singletonList(jobPtInfo));

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                                 MockHiveMetaSource.TBL_PARTITIONED);
    String partitionValuesJson =
        GsonUtils.getFullConfigGson().toJson(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);
    try (Statement stmt = conn.createStatement()) {
      String sql =
          String.format("SELECT * FROM %s.%s", mmaPartitionMetaSchema, mmaPartitionMetaTable);
      try (ResultSet rs = stmt.executeQuery(sql)) {
        int rowCount = 0;
        while (rs.next()) {
          Assert.assertEquals(partitionValuesJson, rs.getString(1));
          Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING.toString(),
                              rs.getString(2));
          Assert.assertEquals(Constants.MMA_PT_META_INIT_ATTEMPT_TIMES, rs.getInt(3));
          Assert.assertEquals(Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME,
                              rs.getLong(4));
          rowCount += 1;
        }

        Assert.assertEquals(1, rowCount);
      }
    }
  }

  @Test
  public void testDropMmaPartitionMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    MmaMetaManagerDbImplUtils.dropMmaPartitionMeta(conn,
                                                   MockHiveMetaSource.DB_NAME,
                                                   MockHiveMetaSource.TBL_PARTITIONED);

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                                 MockHiveMetaSource.TBL_PARTITIONED);
    try (Statement stmt = conn.createStatement()) {
      String sql = String.format("SHOW TABLES FROM %s", mmaPartitionMetaSchema);
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          Assert.assertNotEquals(mmaPartitionMetaTable, rs.getString(1));
        }
      }
    }
  }

  @Test
  public void testSelectSingleRecordFromMmaPartitionMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                     MockHiveMetaSource.TBL_PARTITIONED);
    String partitionValuesJson = GsonUtils.getFullConfigGson()
        .toJson(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s.%s VALUES('%s', '%s', %d, %d)",
                                 mmaPartitionMetaSchema,
                                 mmaPartitionMetaTable,
                                 partitionValuesJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                                 Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    MmaMetaManagerDbImplUtils.MigrationJobPtInfo jobPtInfo =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(
            conn,
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED,
            MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);

    Assert.assertNotNull(jobPtInfo);
    Assert.assertEquals(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES,
                        jobPtInfo.getPartitionValues());
    Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING,
                        jobPtInfo.getStatus());
    Assert.assertEquals(Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                        jobPtInfo.getAttemptTimes());
    Assert.assertEquals(Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME,
                        jobPtInfo.getLastModifiedTime());
  }

  @Test
  public void testSelectSingleRecordFromMmaPartitionMetaNonExisted() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    MmaMetaManagerDbImplUtils.MigrationJobPtInfo jobPtInfo =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(
            conn,
            MockHiveMetaSource.DB_NAME,
            MockHiveMetaSource.TBL_PARTITIONED,
            MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);

    Assert.assertNull(jobPtInfo);
  }

  @Test
  public void testRecordsSelectFromMmaPartitionMeta() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                                 MockHiveMetaSource.TBL_PARTITIONED);
    String partitionValuesJson = GsonUtils.getFullConfigGson()
        .toJson(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s.%s VALUES('%s', '%s', %d, %d)",
                                 mmaPartitionMetaSchema,
                                 mmaPartitionMetaTable,
                                 partitionValuesJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                                 Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    List<MmaMetaManagerDbImplUtils.MigrationJobPtInfo> migrationJobPtInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                             MockHiveMetaSource.DB_NAME,
                                                             MockHiveMetaSource.TBL_PARTITIONED,
                                                             null,
                                                             -1);

    Assert.assertEquals(1, migrationJobPtInfos.size());
    MmaMetaManagerDbImplUtils.MigrationJobPtInfo migrationJobPtInfo = migrationJobPtInfos.get(0);

    Assert.assertNotNull(migrationJobPtInfo);
    Assert.assertEquals(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES,
                        migrationJobPtInfo.getPartitionValues());
    Assert.assertEquals(MmaMetaManager.MigrationStatus.PENDING, migrationJobPtInfo.getStatus());
    Assert.assertEquals(Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                        migrationJobPtInfo.getAttemptTimes());
    Assert.assertEquals(Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME,
                        migrationJobPtInfo.getLastModifiedTime());
  }

  @Test
  public void testRecordsSelectFromMmaPartitionMetaEmpty() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    List<MmaMetaManagerDbImplUtils.MigrationJobPtInfo> migrationJobPtInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                             MockHiveMetaSource.DB_NAME,
                                                             MockHiveMetaSource.TBL_PARTITIONED,
                                                             null,
                                                             -1);

    Assert.assertEquals(0, migrationJobPtInfos.size());
  }

  @Test
  public void testRecordsSelectFromMmaPartitionMetaWithStatus() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                                 MockHiveMetaSource.TBL_PARTITIONED);
    String partitionValuesJson = GsonUtils.getFullConfigGson()
        .toJson(MockHiveMetaSource.TBL_PARTITIONED_PARTITION_VALUES);
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("INSERT INTO %s.%s VALUES('%s', '%s', %d, %d)",
                                 mmaPartitionMetaSchema,
                                 mmaPartitionMetaTable,
                                 partitionValuesJson,
                                 MmaMetaManager.MigrationStatus.PENDING.toString(),
                                 Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                                 Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME);
      stmt.execute(dml);
    }

    List<MmaMetaManagerDbImplUtils.MigrationJobPtInfo> migrationJobPtInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                             MockHiveMetaSource.DB_NAME,
                                                             MockHiveMetaSource.TBL_PARTITIONED,
                                                             MmaMetaManager.MigrationStatus.FAILED,
                                                             -1);

    Assert.assertEquals(0, migrationJobPtInfos.size());
  }

  @Test
  public void testRecordsSelectFromMmaPartitionMetaWithLimit() throws SQLException {
    MmaMetaManagerDbImplUtils.createMmaPartitionMetaSchema(conn, MockHiveMetaSource.DB_NAME);
    MmaMetaManagerDbImplUtils.createMmaPartitionMeta(conn,
                                                     MockHiveMetaSource.DB_NAME,
                                                     MockHiveMetaSource.TBL_PARTITIONED);

    String mmaPartitionMetaSchema = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT,
                                                  MockHiveMetaSource.DB_NAME);
    String mmaPartitionMetaTable = String.format(Constants.MMA_PT_META_TBL_NAME_FMT,
                                                 MockHiveMetaSource.TBL_PARTITIONED);
    try (Statement stmt = conn.createStatement()) {
      for (int i = 0; i < 10; i++) {
        String dml = String.format("INSERT INTO %s.%s VALUES('%s', '%s', %d, %d)",
                                   mmaPartitionMetaSchema,
                                   mmaPartitionMetaTable,
                                   GsonUtils.getFullConfigGson().toJson(Collections.singletonList(Integer.toString(i))),
                                   MmaMetaManager.MigrationStatus.PENDING.toString(),
                                   Constants.MMA_PT_META_INIT_ATTEMPT_TIMES,
                                   Constants.MMA_PT_MEAT_NA_LAST_MODIFIED_TIME);
        stmt.execute(dml);
      }
    }

    List<MmaMetaManagerDbImplUtils.MigrationJobPtInfo> migrationJobPtInfos =
        MmaMetaManagerDbImplUtils.selectFromMmaPartitionMeta(conn,
                                                             MockHiveMetaSource.DB_NAME,
                                                             MockHiveMetaSource.TBL_PARTITIONED,
                                                             MmaMetaManager.MigrationStatus.PENDING,
                                                             1);

    Assert.assertEquals(1, migrationJobPtInfos.size());
  }
}
