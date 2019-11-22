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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * IntermediateDataManager reads/writes report/odps ddl/hive sql from/to a directory. The
 * structure of generated directory is as follows:
 *
 *  [output directory]
 *  |______Report.html
 *  |______[database name]
 *         |______[table name]
 *                |______odps_ddl
 *                |      |______create_table.sql
 *                |      |______[partition spec].sql
 *                |      |______...
 *                |______hive_udtf_sql
 *                       |______single_partition
 *                       |      |______[partition spec].sql
 *                       |      |______...
 *                       |______multi_partition
 *                              |______[table name].sql
 */
public class IntermediateDataManager {

  /**
   * Directory & file names
   */
  private static final String ODPS_DDL_DIR = "odps_ddl";
  private static final String HIVE_UDTF_DIR = "hive_udtf_sql";
  private static final String HIVE_VERIFY_DIR = "hive_verify_sql";
  private static final String ODPS_VERIFY_DIR = "odps_verify_sql";
  private static final String CREATE_TABLE_FILENAME = "create_table";
  private static final String CREATE_PARTITION_FILENAME = "create_partition_";
  private static final String SINGLE_PARTITION_DIR = "single_partition";
  private static final String MULTI_PARTITION_DIR = "multi_partition";
  private static final String REPROT = "Report.html";
  private static final String SQL_SUFFIX = ".sql";

  private String root;

  public IntermediateDataManager(String root) {
    this.root = root;
  }

  public String getOdpsCreateTableStatement(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              ODPS_DDL_DIR,
                              CREATE_TABLE_FILENAME + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setOdpsCreateTableStatement(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              ODPS_DDL_DIR,
                              CREATE_TABLE_FILENAME + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public List<String> getOdpsAddPartitionStatements(String databaseName,
                                                   String tableName) throws IOException {
    List<String> addPartitionStatements = new ArrayList<>();

    Path odpsDdlDir = Paths.get(this.root, databaseName, tableName, ODPS_DDL_DIR);
    String[] filenames = DirUtils.listFiles(odpsDdlDir, CREATE_PARTITION_FILENAME);
    for (String filename : filenames) {
      Path filePath = Paths.get(this.root, databaseName, tableName, ODPS_DDL_DIR, filename);
      addPartitionStatements.add(DirUtils.readFromFile(filePath));
    }
    return addPartitionStatements;
  }

  public void setOdpsAddPartitionStatement(String databaseName,
                                           String tableName,
                                           String partitionSpec,
                                           String addPartitionStatement) throws IOException {
    String filename = CREATE_PARTITION_FILENAME + partitionSpec + SQL_SUFFIX;
    Path filePath = Paths.get(this.root, databaseName, tableName, ODPS_DDL_DIR, filename);
    DirUtils.writeToFile(filePath, addPartitionStatement);
  }

  public List<String> getHiveUdtfSqlSinglePartition(String databaseName,
                                               String tableName) throws IOException {
    List<String> hiveUdtfSqlSinglePartition = new ArrayList<>();
    Path hiveSinglePartitionUdtfSqlDir = Paths.get(this.root,
                                                   databaseName,
                                                   tableName,
                                                   HIVE_UDTF_DIR,
                                                   SINGLE_PARTITION_DIR);
    String[] filenames = DirUtils.listFiles(hiveSinglePartitionUdtfSqlDir);
    for (String filename : filenames) {
      Path filePath = Paths.get(this.root,
                                databaseName,
                                tableName,
                                HIVE_UDTF_DIR,
                                SINGLE_PARTITION_DIR,
                                filename);
      hiveUdtfSqlSinglePartition.add( DirUtils.readFromFile(filePath));
    }
    return hiveUdtfSqlSinglePartition;
  }

  public void setHiveUdtfSqlSinglePartition(String databaseName,
                                            String tableName,
                                            String partitionSpec,
                                            String hiveSql) throws IOException {
    String filename =  partitionSpec + SQL_SUFFIX;
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              HIVE_UDTF_DIR,
                              SINGLE_PARTITION_DIR,
                              filename);
    DirUtils.writeToFile(filePath, hiveSql);
  }

  public void setHiveVerifySqlSinglePartition(String databaseName,
                                              String tableName,
                                              String partitionSpec,
                                              String hiveSql) throws IOException {
    String filename =  partitionSpec + SQL_SUFFIX;
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              HIVE_VERIFY_DIR,
                              SINGLE_PARTITION_DIR,
                              filename);
    DirUtils.writeToFile(filePath, hiveSql);
  }

  public void setOdpsVerifySqlSinglePartition(String databaseName,
                                              String tableName,
                                              String partitionSpec,
                                              String odpsSql) throws IOException {
    String filename =  partitionSpec + SQL_SUFFIX;
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              ODPS_VERIFY_DIR,
                              SINGLE_PARTITION_DIR,
                              filename);
    DirUtils.writeToFile(filePath, odpsSql);
  }

  public String getHiveUdtfSqlMultiPartition(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              HIVE_UDTF_DIR,
                              MULTI_PARTITION_DIR,
                              tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setHiveUdtfSqlMultiPartition(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              HIVE_UDTF_DIR,
                              MULTI_PARTITION_DIR,
                              tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public void setHiveVerifySqlWholeTable(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              HIVE_VERIFY_DIR,
                              tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public void setOdpsVerifySqlWholeTable(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root,
                              databaseName,
                              tableName,
                              ODPS_VERIFY_DIR,
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

  public String[] listDatabases() {
    Path rootDir = Paths.get(this.root);
    if (!Files.exists(rootDir)) {
      return new String[0];
    }
    return DirUtils.listDirs(rootDir);
  }

  public String[] listTables(String databaseName) {
    Path databaseDir = Paths.get(this.root, databaseName);

    return DirUtils.listDirs(databaseDir);
  }
}
