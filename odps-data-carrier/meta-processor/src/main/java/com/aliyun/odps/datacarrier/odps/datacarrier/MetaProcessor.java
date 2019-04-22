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

package com.aliyun.odps.datacarrier.odps.datacarrier;

import com.aliyun.odps.datacarrier.commons.Constants.DATASOURCE_TYPE;
import com.aliyun.odps.datacarrier.commons.IntermediateDataDirManager;
import com.aliyun.odps.datacarrier.commons.MetaManager;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.DatabaseMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: jon (wangzhong.zw@alibaba-inc.com)
 *
 * usage:
 * java -cp /path/to/jar com.aliyun.odps.datacarrier.odps.MetaProcessor [meta file path]
 * [config file path] [result directory path]
 *
 */
public class MetaProcessor {
  private MetaManager metaManager;

  public MetaProcessor(String metaPath) throws IOException {
    this.metaManager = new MetaManager(metaPath);
  }

  private void run(String outputPath) throws IOException {
    IntermediateDataDirManager intermediateDataDirManager =
        new IntermediateDataDirManager(outputPath);

    GlobalMetaModel globalMeta = metaManager.getGlobalMeta();
    for (String databaseName : metaManager.listDatabases()) {
      DatabaseMetaModel databaseMeta = metaManager.getDatabaseMeta(databaseName);

      for (String tableName : metaManager.listTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, tableName);

        // Generate ODPS create table statements
        String tableDescription = getTableDescription(databaseMeta, tableMeta);
        String createTableStatement = getCreateTableStatement(globalMeta, databaseMeta, tableMeta);
        intermediateDataDirManager.setOdpsCreateTableStatement(databaseName, tableName,
            tableDescription + createTableStatement);

        String multiPartitionHiveUdtfSQL = getMultiPartitionHiveUdtfSQL(databaseMeta, tableMeta);
        intermediateDataDirManager.setHiveUdtfSQLMultiPartition(
            databaseName, tableName, multiPartitionHiveUdtfSQL);
      }

      for (String partitionTableName : metaManager.listPartitionTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, partitionTableName);

        // Generate ODPS add partition statements
        List<String> addPartitionStatements =
            getCreatePartitionStatements(globalMeta, databaseMeta, tableMeta);
        intermediateDataDirManager.setOdpsAddPartitionStatement(databaseName, partitionTableName,
            String.join("\n", addPartitionStatements));

        // Generate Hive UDTF SQL statements
        List<String> singlePartitionHiveUdtfSQL =
            getSinglePartitionHiveUdtfSQL(databaseMeta, tableMeta);
        String multiPartitionHiveUdtfSQL = getMultiPartitionHiveUdtfSQL(databaseMeta, tableMeta);
        intermediateDataDirManager.setHiveUdtfSQLSinglePartition(databaseName, partitionTableName,
            String.join("\n", singlePartitionHiveUdtfSQL));
      }
    }

  }

  // TODO: includes more info, like original compatibility and risks
  private String getTableDescription(DatabaseMetaModel databaseMeta, TableMetaModel tableMeta) {
    StringBuilder commentBuilder = new StringBuilder();
    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;
    commentBuilder
        .append("--********************************************************************--\n")
        .append("--project name: ").append(odpsProjectName).append("\n")
        .append("--table name: ").append(odpsTableName).append("\n")
        .append("--********************************************************************--\n");
    return commentBuilder.toString();
  }

  // TODO: instead of return a string, return a TransformResult object
  public static String getCreateTableStatement(GlobalMetaModel globalMeta,
      DatabaseMetaModel databaseMeta, TableMetaModel tableMeta) {
    DATASOURCE_TYPE datasourceType = DATASOURCE_TYPE.valueOf(globalMeta.datasourceType);
    String odpsVersion = globalMeta.odpsVersion;

    StringBuilder ddlBuilder = new StringBuilder();
    if ("2.0".equals(odpsVersion)) {
      ddlBuilder.append("set odps.sql.type.system.odps2=true;\n");
    }

    ddlBuilder.append("CREATE TABLE ");

    if (tableMeta.ifNotExist) {
      ddlBuilder.append(" IF NOT EXISTS ");
    }

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;
    ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("` (\n");

    List<ColumnMetaModel> columns = tableMeta.columns;
    for (int i = 0; i < columns.size(); i++) {
      ColumnMetaModel columnMeta = columns.get(i);

      String odpsType;
      if (datasourceType.equals(DATASOURCE_TYPE.MYSQL)) {
        odpsType = MySQLTypeTransformer.toOdpsType(columnMeta.type, odpsVersion);
      } else if (datasourceType.equals(DATASOURCE_TYPE.HIVE)) {
        odpsType = HiveTypeTransformer.toOdpsType(columnMeta.type, odpsVersion);
      } else {
        throw new IllegalArgumentException("Unsupported datasource type: " + datasourceType);
      }

      String columnComment = columnMeta.comment;
      ddlBuilder.append("    `").append(columnMeta.odpsColumnName).append("` ").append(odpsType);
      if (columnComment != null) {
        ddlBuilder.append(" COMMENT '").append(columnComment).append("'");
      }
      if (i + 1 < columns.size()) {
        ddlBuilder.append(",\n");
      }
    }
    ddlBuilder.append(")");

    if (tableMeta.comment != null) {
      ddlBuilder.append("\nCOMMENT '").append(tableMeta.comment).append("'\n");
    }

    List<ColumnMetaModel> partitionColumns = tableMeta.partitionColumns;
    if (partitionColumns != null && partitionColumns.size() > 0) {
      ddlBuilder.append("PARTITIONED BY (\n");
      for (int i = 0; i < partitionColumns.size(); i++) {
        ColumnMetaModel partitionColumnMeta = partitionColumns.get(i);

        String odpsType = null;
        if (datasourceType.equals(DATASOURCE_TYPE.MYSQL)) {
          odpsType = MySQLTypeTransformer.toOdpsType(partitionColumnMeta.type, odpsVersion);
        } else if (datasourceType.equals(DATASOURCE_TYPE.HIVE)) {
          odpsType = HiveTypeTransformer.toOdpsType(partitionColumnMeta.type, odpsVersion);
        }

        String columnComment = partitionColumnMeta.comment;
        String odpsPartitionColumnName = partitionColumnMeta.odpsColumnName;
        ddlBuilder.append("    `").append(odpsPartitionColumnName).append("` ").append(odpsType);
        if (columnComment != null) {
          ddlBuilder.append(" COMMENT '").append(columnComment).append("'");
        }
        if (i + 1 < partitionColumns.size()) {
          ddlBuilder.append(",\n");
        }
      }
      ddlBuilder.append(")");
    }

    if (tableMeta.lifeCycle != null) {
      ddlBuilder.append("\nLIFECYCLE ").append(tableMeta.lifeCycle);
    }

    ddlBuilder.append(";\n");

    return ddlBuilder.toString();
  }

  // TODO: instead of return a list of string, return a TransformResult object
  private List<String> getCreatePartitionStatements(GlobalMetaModel globalMeta,
      DatabaseMetaModel databaseMeta, TableMetaModel tableMeta) throws IOException{
    List<String> createPartitionStatements = new ArrayList<>();

    String odpsVersion = globalMeta.odpsVersion;
    if ("2.0".equals(odpsVersion)) {
      createPartitionStatements.add("set odps.sql.type.system.odps2=true;\n");
    }

    TablePartitionMetaModel tablePartitionMeta =
        metaManager.getTablePartitionMeta(databaseMeta.databaseName, tableMeta.tableName);
    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      StringBuilder ddlBuilder = new StringBuilder();
      ddlBuilder.append("ALTER TABLE ");

      String odpsProjectName = databaseMeta.odpsProjectName;
      String odpsTableName = tableMeta.odpsTableName;
      ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("` ");
      ddlBuilder.append("ADD PARTITION (").append(partitionMeta.partitionSpec).append(");\n");

      createPartitionStatements.add(ddlBuilder.toString());
    }
    return createPartitionStatements;
  }

  private List<String> getSinglePartitionHiveUdtfSQL(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta) throws IOException {
    TablePartitionMetaModel tablePartitionMeta =
        metaManager.getTablePartitionMeta(databaseMeta.databaseName, tableMeta.tableName);
    List<String> hiveSQLList = new ArrayList<>();

    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      String odpsTableName = tableMeta.odpsTableName;
      List<String> hiveColumnNames = new ArrayList<>();
      List<String> odpsColumnNames = new ArrayList<>();
      for (ColumnMetaModel columnMeta : tableMeta.columns) {
        odpsColumnNames.add(columnMeta.odpsColumnName);
        hiveColumnNames.add(columnMeta.columnName);
      }

      StringBuilder hiveUdtfSQLBuilder = new StringBuilder();
      hiveUdtfSQLBuilder.append("SELECT odps_data_dump(\n")
          .append("\'").append(odpsTableName).append("\',\n")
          .append("\'").append(partitionMeta.partitionSpec).append("\',\n")
          .append("\'").append(String.join(",", odpsColumnNames)).append("\',\n");
      for (int i = 0; i < hiveColumnNames.size(); i++) {
        hiveUdtfSQLBuilder.append("`").append(hiveColumnNames.get(i)).append("`");
        if (i != hiveColumnNames.size() - 1) {
          hiveUdtfSQLBuilder.append(",\n");
        } else {
          hiveUdtfSQLBuilder.append(")\n");
        }
      }
      hiveUdtfSQLBuilder.append("FROM\n")
          .append(databaseMeta.databaseName).append("`").append(tableMeta.tableName).append("`")
          .append("WHERE ").append(partitionMeta.partitionSpec).append(";\n");
      hiveSQLList.add(hiveUdtfSQLBuilder.toString());
    }

    return hiveSQLList;
  }

  private String getMultiPartitionHiveUdtfSQL(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta) {
    StringBuilder hiveUdtfSQLBuilder = new StringBuilder();

    List<String> hiveColumnNames = new ArrayList<>();
    List<String> odpsColumnNames = new ArrayList<>();
    for (ColumnMetaModel columnMeta : tableMeta.columns) {
      odpsColumnNames.add(columnMeta.odpsColumnName);
      hiveColumnNames.add(columnMeta.columnName);
    }
    List<String> odpsPartitionColumnNames = new ArrayList<>();
    for (ColumnMetaModel columnMeta : tableMeta.partitionColumns) {
      odpsPartitionColumnNames.add(columnMeta.odpsColumnName);
      hiveColumnNames.add(columnMeta.columnName);
    }
    hiveUdtfSQLBuilder.append("SELECT odps_data_dump(\n")
        .append("\'").append(tableMeta.odpsTableName).append("\',\n")
        .append("\'").append(String.join(",", odpsColumnNames)).append("\',\n")
        .append("\'").append(String.join(",", odpsPartitionColumnNames)).append("\',\n");
    for (int i = 0; i < hiveColumnNames.size(); i++) {
      hiveUdtfSQLBuilder.append("`").append(hiveColumnNames.get(i)).append("`");
      if (i != hiveColumnNames.size() - 1) {
        hiveUdtfSQLBuilder.append(",\n");
      } else {
        hiveUdtfSQLBuilder.append(")\n");
      }
    }
    String databaseName = databaseMeta.databaseName;
    String tableName = tableMeta.tableName;
    hiveUdtfSQLBuilder.append("FROM\n")
        .append(databaseName).append("`").append(tableName).append("`").append(";\n");

    return hiveUdtfSQLBuilder.toString();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("arguments: \n" +
          "<meta file> <output dir>");
    }

    MetaProcessor metaProcessor = new MetaProcessor(args[0]);
    metaProcessor.run(args[1]);
  }
}
