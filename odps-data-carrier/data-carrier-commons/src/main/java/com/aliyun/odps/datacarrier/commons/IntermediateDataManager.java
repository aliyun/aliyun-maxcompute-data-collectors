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

package com.aliyun.odps.datacarrier.commons;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * IntermediateDataManager reads/writes report/odps ddl/hive sql from/to a directory. The
 * structure of generated directory is as follows:
 *
 *  [output directory]
 *  |______Report.html
 *  |______[database name]
 *         |______odps_ddl
 *         |      |______tables
 *         |      |      |______[table name].sql
 *         |      |______partitions
 *         |             |______[table name].sql
 *         |______hive_udtf_sql
 *                |______single_partition
 *                |      |______[table name].sql
 *                |______multi_partition
 *                       |______[table name].sql
 */
public class IntermediateDataManager {

  /**
   * Directory & file names
   */
  private static final String ODPS_DDL_DIR = "odps_ddl";
  private static final String TABLE_META_DIR = "tables";
  private static final String PARTITION_META_DIR = "partitions";
  private static final String HIVE_UDTF_DIR = "hive_udtf_sql";
  private static final String SINGLE_PARTITION_DIR = "single_partition";
  private static final String MULTI_PARTITION_DIR = "multi_partition";
  private static final String REPROT = "report.html";

  private static final String SQL_SUFFIX = ".sql";

  private String root;

  public IntermediateDataManager(String root) {
    this.root = root;
  }

  public String getOdpsCreateTableStatement(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, ODPS_DDL_DIR, TABLE_META_DIR,
        tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setOdpsCreateTableStatement(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, ODPS_DDL_DIR, TABLE_META_DIR,
        tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getOdpsAddPartitionStatement(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, ODPS_DDL_DIR, PARTITION_META_DIR,
        tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setOdpsAddPartitionStatement(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, ODPS_DDL_DIR, PARTITION_META_DIR,
        tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getHiveUdtfSQLSinglePartition(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, ODPS_DDL_DIR, SINGLE_PARTITION_DIR,
        tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setHiveUdtfSQLSinglePartition(String databaseName, String tableName,
      String content) throws IOException {
    Path filePath = Paths.get(this.root, databaseName, HIVE_UDTF_DIR, SINGLE_PARTITION_DIR,
        tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getHiveUdtfSQLMultiPartition(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, HIVE_UDTF_DIR, MULTI_PARTITION_DIR,
        tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setHiveUdtfSQLMultiPartition(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, databaseName, HIVE_UDTF_DIR, MULTI_PARTITION_DIR,
        tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public void setReport(String content) throws IOException {
    Path filePath = Paths.get(this.root, REPROT);
    DirUtils.writeToFile(filePath, content);
  }

  public String getReport() throws IOException {
    Path filePath = Paths.get(this.root, REPROT);
    return DirUtils.readFromFile(filePath);
  }
}
