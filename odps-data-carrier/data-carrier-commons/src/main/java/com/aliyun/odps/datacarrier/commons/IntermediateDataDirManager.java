package com.aliyun.odps.datacarrier.commons;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * IntermediateDataDirManager reads/writes report/odps ddl/hive sql from/to a directory. The
 * structure of generated directory is as follows:
 *
 *  [output directory]
 *  |______data-carrier
 *         |______Report.html
 *         |______odps_ddl
 *         |      |______tables
 *         |             |______[database name]
 *         |                    |______[table name].sql
 *         |      |______partitions
 *         |             |______[database name]
 *         |                    |______[table name].sql
 *         |______hive_udtf_sql
 *                |______single_partition
 *                |      |______[database name]
 *                |             |______[table name].sql
 *                |
 *                |______multi_partition
 *                       |______[database name]
 *                              |______[table name].sql
 */
public class IntermediateDataDirManager {

  /**
   * Directory & file names
   */
  private static final String WORKSPACE = "data-carrier";
  private static final String ODPS_DDL_DIR = "odps_ddl";
  private static final String TABLE_META_DIR = "tables";
  private static final String PARTITION_META_DIR = "partitions";
  private static final String HIVE_UDTF_DIR = "hive_udtf_sql";
  private static final String SINGLE_PARTITION_DIR = "single_partition";
  private static final String MULTI_PARTITION_DIR = "multi_partition";
  private static final String REPROT = "report.html";

  private static final String SQL_SUFFIX = ".sql";

  private String root;

  public IntermediateDataDirManager(String root) {
    this.root = root;
  }

  public String getOdpsCreateTableStatement(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, ODPS_DDL_DIR, TABLE_META_DIR,
        databaseName, tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setOdpsCreateTableStatement(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, ODPS_DDL_DIR, TABLE_META_DIR,
        databaseName, tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getOdpsAddPartitionStatement(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, ODPS_DDL_DIR, PARTITION_META_DIR,
        databaseName, tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setOdpsAddPartitionStatement(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, ODPS_DDL_DIR, PARTITION_META_DIR,
        databaseName, tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getHiveUdtfSQLSinglePartition(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, ODPS_DDL_DIR, SINGLE_PARTITION_DIR,
        databaseName, tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setHiveUdtfSQLSinglePartition(String databaseName, String tableName,
      String content) throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, HIVE_UDTF_DIR, SINGLE_PARTITION_DIR,
        databaseName, tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public String getHiveUdtfSQLMultiPartition(String databaseName, String tableName)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, HIVE_UDTF_DIR, MULTI_PARTITION_DIR,
        databaseName, tableName + SQL_SUFFIX);
    return DirUtils.readFromFile(filePath);
  }

  public void setHiveUdtfSQLMultiPartition(String databaseName, String tableName, String content)
      throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, HIVE_UDTF_DIR, MULTI_PARTITION_DIR,
        databaseName, tableName + SQL_SUFFIX);
    DirUtils.writeToFile(filePath, content);
  }

  public void setReport(String content) throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, REPROT);
    DirUtils.writeToFile(filePath, content);
  }

  public String getReport() throws IOException {
    Path filePath = Paths.get(this.root, WORKSPACE, REPROT);
    return DirUtils.readFromFile(filePath);
  }


}
