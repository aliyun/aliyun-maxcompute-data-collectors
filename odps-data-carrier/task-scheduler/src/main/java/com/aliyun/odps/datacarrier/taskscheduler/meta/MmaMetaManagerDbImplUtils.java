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

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager.MigrationStatus;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.google.gson.reflect.TypeToken;

public class MmaMetaManagerDbImplUtils {

  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerDbImplUtils.class);

  /**
   * Represents a row in table meta
   */
  public static class JobInfo {
    private String db;
    private String tbl;
    private boolean isPartitioned;
    private MmaConfig.JobConfig jobConfig;
    private MigrationStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;

    public JobInfo(String db,
                   String tbl,
                   boolean isPartitioned,
                   MmaConfig.JobConfig jobConfig,
                   MigrationStatus status,
                   int attemptTimes,
                   long lastModifiedTime) {
      this.db = Objects.requireNonNull(db);
      this.tbl = Objects.requireNonNull(tbl);
      this.isPartitioned = isPartitioned;
      this.jobConfig = Objects.requireNonNull(jobConfig);
      this.status = Objects.requireNonNull(status);
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getDb() {
      return db;
    }

    public String getTbl() {
      return tbl;
    }

    public boolean isPartitioned() {
      return isPartitioned;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public MigrationStatus getStatus() {
      return status;
    }

    public MmaConfig.JobConfig getJobConfig() {
      return jobConfig;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public void setStatus(MigrationStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  public static class RestoreTaskInfo {
    private String uniqueId;
    private String type;
    private String db;
    private String object;
    private MmaConfig.JobConfig jobConfig;
    private MigrationStatus status;
    private int attemptTimes;
    private long lastModifiedTime;

    public RestoreTaskInfo(String uniqueId,
                           String type,
                           String db,
                           String object,
                           MmaConfig.JobConfig jobConfig,
                           MigrationStatus status,
                           int attemptTimes,
                           long lastModifiedTime) {
      this.uniqueId = uniqueId;
      this.type = type;
      this.db = db;
      this.object = object;
      this.jobConfig = jobConfig;
      this.status = status;
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastModifiedTime;
    }

    public String getUniqueId() {
      return uniqueId;
    }

    public String getType() {
      return type;
    }

    public String getDb() {
      return db;
    }

    public String getObject() {
      return object;
    }

    public MmaConfig.JobConfig getJobConfig() {
      return jobConfig;
    }

    public MigrationStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MigrationStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  /**
   * Represents a row in partition meta
   */
  public static class MigrationJobPtInfo {
    private List<String> partitionValues;
    private MigrationStatus status;
    private int attemptTimes;
    private Long lastModifiedTime;

    public MigrationJobPtInfo(List<String> partitionValues,
                              MigrationStatus status,
                              int attemptTimes,
                              long lastSuccTimestamp) {
      this.partitionValues = Objects.requireNonNull(partitionValues);
      this.status = Objects.requireNonNull(status);
      this.attemptTimes = attemptTimes;
      this.lastModifiedTime = lastSuccTimestamp;
    }

    public List<String> getPartitionValues() {
      return partitionValues;
    }

    public MigrationStatus getStatus() {
      return status;
    }

    public int getAttemptTimes() {
      return attemptTimes;
    }

    public long getLastModifiedTime() {
      return lastModifiedTime;
    }

    public void setStatus(MigrationStatus status) {
      this.status = status;
    }

    public void setAttemptTimes(int attemptTimes) {
      this.attemptTimes = attemptTimes;
    }

    public void setLastModifiedTime(long lastModifiedTime) {
      this.lastModifiedTime = lastModifiedTime;
    }
  }

  public static String getCreateMmaPartitionMetaSchemaDdl(String db) {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    return "CREATE SCHEMA IF NOT EXISTS " + schemaName;
  }

  public static String getCreateMmaTableMetaDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(Constants.MMA_TBL_META_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_TBL_META_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append(Constants.MMA_TBL_META_COL_DB_NAME).append(", ");
    sb.append(Constants.MMA_TBL_META_COL_TBL_NAME).append("))\n");
    return sb.toString();
  }

  public static String getCreateMmaRestoreTableDdl() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(Constants.MMA_OBJ_RESTORE_TBL_NAME).append(" (\n");
    for (Map.Entry<String, String> entry : Constants.MMA_OBJ_RESTORE_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append(Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_TYPE).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_DB_NAME).append(", ");
    sb.append(Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME).append("))\n");
    return sb.toString();
  }

  public static String getCreateMmaPartitionMetaDdl(String db, String tbl) {
    StringBuilder sb = new StringBuilder();
    sb
        .append("CREATE TABLE IF NOT EXISTS ")
        .append(String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db))
        .append(".")
        .append(String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl))
        .append(" (\n");

    for (Map.Entry<String, String> entry : Constants.MMA_PT_META_COL_TO_TYPE.entrySet()) {
      sb.append("    ").append(entry.getKey()).append(" ").append(entry.getValue()).append(",\n");
    }
    sb.append("    PRIMARY KEY (").append(Constants.MMA_PT_META_COL_PT_VALS).append("))\n");
    return sb.toString();
  }

  public static void createMmaTableMeta(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaTableMetaDdl();
      LOG.debug("Executing create table ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void createMmaRestoreTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaRestoreTableDdl();
      LOG.debug("Executing create table ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void removeActiveTasksFromRestoreTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String dml = String.format("DELETE FROM %s WHERE %s='%s' or %s='%s'",
          Constants.MMA_OBJ_RESTORE_TBL_NAME,
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          MigrationStatus.PENDING.name(),
          Constants.MMA_OBJ_RESTORE_COL_STATUS,
          MigrationStatus.RUNNING.name());
      LOG.info("Executing delete rows ddl: {}", dml);
      stmt.execute(dml);
    }
  }

  public static void createMmaPartitionMetaSchema(Connection conn, String db) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaPartitionMetaSchemaDdl(db);
      LOG.info("Executing create schema ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  public static void createMmaPartitionMeta(Connection conn,
                                            String db,
                                            String tbl) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      String ddl = getCreateMmaPartitionMetaDdl(db, tbl);
      LOG.info("Executing create schema ddl: {}", ddl);
      stmt.execute(ddl);
    }
  }

  /**
   * Insert into or update (A.K.A Upsert) MMA_TBL_META
   */
  public static void mergeIntoMmaTableMeta(Connection conn, JobInfo jobInfo)
      throws SQLException {

    String dml = "MERGE INTO " + Constants.MMA_TBL_META_TBL_NAME + " VALUES (?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, jobInfo.getDb());
      preparedStatement.setString(2, jobInfo.getTbl());
      preparedStatement.setBoolean(3, jobInfo.isPartitioned());
      preparedStatement.setString(4,
                                  GsonUtils.getFullConfigGson().toJson(jobInfo.getJobConfig()));
      preparedStatement.setString(5, jobInfo.getStatus().toString());
      preparedStatement.setInt(6, jobInfo.getAttemptTimes());
      preparedStatement.setLong(7, jobInfo.getLastModifiedTime());

      LOG.info("Executing DML: {}, arguments: {}",
               dml,
               GsonUtils.getFullConfigGson().toJson(jobInfo));

      preparedStatement.execute();
    }
  }

  public static void mergeIntoRestoreTableMeta(Connection conn, RestoreTaskInfo taskInfo)
      throws SQLException {
    String dml = "MERGE INTO " + Constants.MMA_OBJ_RESTORE_TBL_NAME + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      preparedStatement.setString(1, taskInfo.getUniqueId());
      preparedStatement.setString(2, taskInfo.getType());
      preparedStatement.setString(3, taskInfo.getDb());
      preparedStatement.setString(4, taskInfo.getObject());
      preparedStatement.setString(5,
                                  GsonUtils.getFullConfigGson().toJson(taskInfo.getJobConfig()));
      preparedStatement.setString(6, taskInfo.getStatus().toString());
      preparedStatement.setInt(7, taskInfo.getAttemptTimes());
      preparedStatement.setLong(8, taskInfo.getLastModifiedTime());

      LOG.info("Executing DML: {}, arguments: {}",
               dml,
               GsonUtils.getFullConfigGson().toJson(taskInfo));

      preparedStatement.execute();
    }
  }

  /**
   * Delete from MMA_META
   */
  public static void deleteFromMmaMeta(Connection conn, String db, String tbl) throws SQLException {
    String dml = String.format("DELETE FROM %s WHERE %s='%s' and %s='%s'",
                               Constants.MMA_TBL_META_TBL_NAME,
                               Constants.MMA_TBL_META_COL_DB_NAME,
                               db,
                               Constants.MMA_TBL_META_COL_TBL_NAME,
                               tbl);
    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing DML: {}", dml);
      stmt.execute(dml);
    }
  }

  /**
   * Return a record from MMA_TBL_META if it exists, else null
   */
  public static JobInfo selectFromMmaTableMeta(Connection conn, String db, String tbl)
      throws SQLException {

    String sql = String.format("SELECT * FROM %s WHERE %s='%s' and %s='%s'",
                               Constants.MMA_TBL_META_TBL_NAME,
                               Constants.MMA_TBL_META_COL_DB_NAME,
                               db,
                               Constants.MMA_TBL_META_COL_TBL_NAME,
                               tbl);

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if(!rs.next()) {
          return null;
        }
        return new JobInfo(db,
                                tbl,
                                rs.getBoolean(3),
                                GsonUtils.getFullConfigGson().fromJson(rs.getString(4), MmaConfig.JobConfig.class),
                                MigrationStatus.valueOf(rs.getString(5)),
                                rs.getInt(6),
                                rs.getLong(7));
      }
    }
  }

  /**
   * Return records from MMA_TBL_META
   */
  public static List<JobInfo> selectFromMmaTableMeta(Connection conn,
                                                     MigrationStatus status,
                                                     int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s", Constants.MMA_TBL_META_TBL_NAME));
    if (status != null) {
      sb.append(String.format(" WHERE %s='%s'",
          Constants.MMA_PT_META_COL_STATUS,
          status.toString()));
    }
    sb.append(String.format(" ORDER BY %s, %s DESC",
        Constants.MMA_TBL_META_COL_DB_NAME,
        Constants.MMA_TBL_META_COL_TBL_NAME));
    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sb.toString());

      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<JobInfo> ret = new LinkedList<>();
        while (rs.next()) {
          JobInfo jobInfo =
              new JobInfo(rs.getString(1),
                  rs.getString(2),
                  rs.getBoolean(3),
                  GsonUtils.getFullConfigGson().fromJson(rs.getString(4), MmaConfig.JobConfig.class),
                  MigrationStatus.valueOf(rs.getString(5)),
                  rs.getInt(6),
                  rs.getLong(7));
          ret.add(jobInfo);
        }
        return ret;
      }
    }
  }

  public static List<RestoreTaskInfo> selectFromRestoreMeta(Connection conn,
                                                            String condition,
                                                            int limit) throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT * FROM %s\n", Constants.MMA_OBJ_RESTORE_TBL_NAME));
    if (!Strings.isNullOrEmpty(condition)) {
      sb.append(condition).append("\n");
    }
    sb.append(String.format("ORDER BY %s, %s, %s, %s DESC\n",
                            Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID,
                            Constants.MMA_OBJ_RESTORE_COL_DB_NAME,
                            Constants.MMA_OBJ_RESTORE_COL_OBJECT_NAME,
                            Constants.MMA_OBJ_RESTORE_COL_TYPE));
    if (limit > 0) {
      sb.append("LIMIT ").append(limit);
    }
    sb.append(";");

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sb.toString());
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<RestoreTaskInfo> ret = new LinkedList<>();
        while (rs.next()) {
          RestoreTaskInfo taskInfo = new RestoreTaskInfo(
              rs.getString(1),
              rs.getString(2),
              rs.getString(3),
              rs.getString(4),
              GsonUtils.getFullConfigGson().fromJson(rs.getString(5), MmaConfig.JobConfig.class),
              MigrationStatus.valueOf(rs.getString(6)),
              rs.getInt(7),
              rs.getLong(8));
          ret.add(taskInfo);
        }
        return ret;
      }
    }
  }

  /**
   * Insert into or update (A.K.A Upsert) MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static void mergeIntoMmaPartitionMeta(Connection conn,
                                               String db,
                                               String tbl,
                                               List<MigrationJobPtInfo> migrationJobPtInfos)
      throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);
    String dml = "MERGE INTO " + schemaName + "." + tableName + " VALUES(?, ?, ?, ?)";

    try (PreparedStatement preparedStatement = conn.prepareStatement(dml)) {
      for (MigrationJobPtInfo jobPtInfo : migrationJobPtInfos) {
        String partitionValuesJson =
            GsonUtils.getFullConfigGson().toJson(jobPtInfo.getPartitionValues());
        preparedStatement.setString(1, partitionValuesJson);
        preparedStatement.setString(2, jobPtInfo.getStatus().toString());
        preparedStatement.setInt(3, jobPtInfo.getAttemptTimes());
        preparedStatement.setLong(4, jobPtInfo.getLastModifiedTime());
        preparedStatement.addBatch();
        LOG.info("Executing DML: {}, arguments: {}",
                 dml,
                 GsonUtils.getFullConfigGson().toJson(jobPtInfo));
      }

      preparedStatement.executeBatch();
    }
  }

  /**
   * Drop table MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static void dropMmaPartitionMeta(Connection conn, String db, String tbl) throws SQLException {
    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);

    String ddl = "DROP TABLE " + schemaName + "." + tableName;
    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing DDL: {}", ddl);

      stmt.execute(ddl);
    }
  }

  /**
   * Return a record from MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl] if it exists, else null
   */
  public static MigrationJobPtInfo selectFromMmaPartitionMeta(Connection conn,
                                                              String db,
                                                              String tbl,
                                                              List<String> partitionValues)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);
    String sql = String.format("SELECT * FROM %s.%s WHERE %s='%s'",
                               schemaName,
                               tableName, Constants.MMA_PT_META_COL_PT_VALS,
                               GsonUtils.getFullConfigGson().toJson(partitionValues));

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if (!rs.next()) {
          return null;
        }
        return new MigrationJobPtInfo(partitionValues,
                                      MigrationStatus.valueOf(rs.getString(2)),
                                      rs.getInt(3),
                                      rs.getLong(4));
      }
    }
  }

  /**
   * Return records from MMA_PT_META_DB_[db].MMA_PT_META_TBL_[tbl]
   */
  public static List<MigrationJobPtInfo> selectFromMmaPartitionMeta(
      Connection conn,
      String db,
      String tbl,
      MigrationStatus status,
      int limit)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM ").append(schemaName).append(".").append(tableName);
    if (status != null) {
      sb.append(String.format(" WHERE %s='%s'",
                              Constants.MMA_PT_META_COL_STATUS,
                              status.toString()));
    }
    sb.append(" ORDER BY ").append(Constants.MMA_PT_META_COL_PT_VALS);
    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    Type type = new TypeToken<List<String>>() {}.getType();
    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sb.toString());

      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        List<MigrationJobPtInfo> ret = new LinkedList<>();
        while (rs.next()) {
          MigrationJobPtInfo jobPtInfo =
              new MigrationJobPtInfo(
                  GsonUtils.getFullConfigGson().fromJson(rs.getString(1), type),
                  MigrationStatus.valueOf(rs.getString(2)),
                  rs.getInt(3),
                  rs.getLong(4));
          ret.add(jobPtInfo);
        }
        return ret;
      }
    }
  }

  public static Map<MigrationStatus, Integer> getPartitionStatusDistribution(
      Connection conn,
      String db,
      String tbl)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);

    StringBuilder sb = new StringBuilder();
    sb
        .append("SELECT ")
        .append(Constants.MMA_PT_META_COL_STATUS).append(", COUNT(1) as CNT FROM ")
        .append(schemaName).append(".").append(tableName)
        .append(" GROUP BY ").append(Constants.MMA_PT_META_COL_STATUS);

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sb.toString());
      Map<MigrationStatus, Integer> ret = new HashMap<>();
      try (ResultSet rs = stmt.executeQuery(sb.toString())) {
        while (rs.next()) {
          MigrationStatus status =
              MigrationStatus.valueOf(rs.getString(1));
          Integer count = rs.getInt(2);
          ret.put(status, count);
        }

        return ret;
      }
    }
  }

  /**
   * Filter out existing partitions from candidates
   */
  public static List<List<String>> filterOutPartitions(
      Connection conn,
      String db,
      String tbl,
      List<List<String>> candidates)
      throws SQLException {

    String schemaName = String.format(Constants.MMA_PT_META_SCHEMA_NAME_FMT, db);
    String tableName = String.format(Constants.MMA_PT_META_TBL_NAME_FMT, tbl);

    String sql = String.format("SELECT %s FROM %s.%s WHERE %s='%s'",
        Constants.MMA_PT_META_COL_PT_VALS,
        schemaName,
        tableName,
        Constants.MMA_PT_META_COL_STATUS,
        MigrationStatus.SUCCEEDED.name());

    try (Statement stmt = conn.createStatement()) {
      LOG.info("Executing SQL: {}", sql);

      try (ResultSet rs = stmt.executeQuery(sql)) {
        Set<String> managedPartitionValuesJsonSet = new HashSet<>();
        while (rs.next()) {
          managedPartitionValuesJsonSet.add(rs.getString(1));
        }

        Type type = new TypeToken<List<String>>() {}.getType();

        // Filter out existing partitions
        return candidates.stream()
            .map(ptv -> GsonUtils.getFullConfigGson().toJson(ptv))
            .filter(v -> !managedPartitionValuesJsonSet.contains(v))
            .map(json -> (List<String>) GsonUtils.getFullConfigGson().fromJson(json, type))
            .collect(Collectors.toList());
      }
    }
  }

  /**
   *  Infer a migration job's status from the statuses of its partitions
   */
  public static MigrationStatus inferPartitionedTableStatus(
      Connection conn,
      String db,
      String tbl)
      throws SQLException {

    Map<MigrationStatus, Integer> statusDistribution =
        MmaMetaManagerDbImplUtils.getPartitionStatusDistribution(conn, db, tbl);
    int total = statusDistribution.values().stream().reduce(0, Integer::sum);
    int pending =
        statusDistribution.getOrDefault(MigrationStatus.PENDING, 0);
    int succeeded =
        statusDistribution.getOrDefault(MigrationStatus.SUCCEEDED, 0);
    int failed =
        statusDistribution.getOrDefault(MigrationStatus.FAILED, 0);

    // Decide table status based on partition status
    if (total == succeeded) {
      return MigrationStatus.SUCCEEDED;
    } else if ((total == succeeded + failed) && failed != 0) {
      return MigrationStatus.FAILED;
    } else if ((total == pending + succeeded + failed) && pending != 0) {
      return MigrationStatus.PENDING;
    } else {
      return MigrationStatus.RUNNING;
    }
  }
}
