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

package com.aliyun.odps.datacarrier.metaprocessor;

import com.aliyun.odps.datacarrier.commons.Constants.ODPS_VERSION;
import com.aliyun.odps.datacarrier.commons.GeneratedStatement;
import com.aliyun.odps.datacarrier.commons.IntermediateDataManager;
import com.aliyun.odps.datacarrier.commons.MetaManager;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.DatabaseMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.risk.Risk;
import com.aliyun.odps.datacarrier.metaprocessor.report.ReportBuilder;
import com.aliyun.odps.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * @author: jon (wangzhong.zw@alibaba-inc.com)
 */
public class MetaProcessor {

  private enum Mode {
    // Transfer from Hive
    Hive,
    // Transfer from external table
    ExternalTable
  }

  private OdpsNameManager nameManager;

  public MetaProcessor() {
    this.nameManager = new OdpsNameManager();
  }

  /**
   * Get a map from hive table name to odps table name. Hive table names are like db_name.tb_name,
   * and odps table names are like prj_name.tb_name.
   *
   * The returned map only contains tables that have been passed to getCreateTableStatement.
   * @return a map from hive table name to odps table name
   */
  public Map<String, String> getHiveTableToOdpsTableMap() {
    return nameManager.getHiveTableToOdpsTableMap();
  }

  /**
   * Given hive meta, generate odps create table statement and risks when running the statement.
   * @param globalMeta see {@link MetaManager}
   * @param databaseMeta see {@link MetaManager}
   * @param tableMeta see {@link MetaManager}
   * @return see {@link GeneratedStatement}
   */
  public GeneratedStatement getCreateTableStatement(GlobalMetaModel globalMeta,
                                                    DatabaseMetaModel databaseMeta,
                                                    TableMetaModel tableMeta) {
    GeneratedStatement generatedStatement = new GeneratedStatement();
    StringBuilder ddlBuilder = new StringBuilder();

    // Register to name manager, check if there is any conflict
    Risk risk = nameManager.add(databaseMeta.databaseName, databaseMeta.odpsProjectName,
        tableMeta.tableName, tableMeta.odpsTableName);
    generatedStatement.setRisk(risk);

    // Enable odps 2.0 data types
    ODPS_VERSION odpsVersion = ODPS_VERSION.valueOf(globalMeta.odpsVersion);
    if (ODPS_VERSION.ODPS_V2.equals(odpsVersion)) {
      ddlBuilder.append("set odps.sql.type.system.odps2=true;\n");
    }

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;

    if (databaseMeta.dropTableIfExists || tableMeta.dropIfExists) {
      ddlBuilder
          .append("DROP TABLE IF EXISTS ")
          .append(odpsProjectName)
          .append(".`")
          .append(odpsTableName)
          .append("`;\n");
    }

    ddlBuilder.append("CREATE TABLE ");

    if (tableMeta.ifNotExists) {
      ddlBuilder.append(" IF NOT EXISTS ");
    }

    ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("` (\n");

    List<ColumnMetaModel> columns = tableMeta.columns;
    for (int i = 0; i < columns.size(); i++) {
      ColumnMetaModel columnMeta = columns.get(i);

      // Transform hive type to odps hive, and note down any incompatibility
      TypeTransformResult typeTransformResult = TypeTransformer.toOdpsType(globalMeta, columnMeta);
      generatedStatement.setRisk(typeTransformResult.getRisk());
      String odpsType = typeTransformResult.getTransformedType();
      ddlBuilder.append("    `").append(columnMeta.odpsColumnName).append("` ").append(odpsType);

      if (columnMeta.comment != null) {
        ddlBuilder.append(" COMMENT '").append(columnMeta.comment).append("'");
      }

      if (i + 1 < columns.size()) {
        ddlBuilder.append(",\n");
      }
    }
    ddlBuilder.append(")\n");

    if (tableMeta.comment != null) {
      ddlBuilder.append("COMMENT '").append(tableMeta.comment).append("'\n");
    }

    List<ColumnMetaModel> partitionColumns = tableMeta.partitionColumns;
    if (partitionColumns != null && partitionColumns.size() > 0) {
      ddlBuilder.append("PARTITIONED BY (\n");
      for (int i = 0; i < partitionColumns.size(); i++) {
        ColumnMetaModel partitionColumnMeta = partitionColumns.get(i);

        // Transform hive type to odps hive, and note down any incompatibility
        TypeTransformResult typeTransformResult =
            TypeTransformer.toOdpsType(globalMeta, partitionColumnMeta);
        generatedStatement.setRisk(typeTransformResult.getRisk());
        String odpsType = typeTransformResult.getTransformedType();
        String odpsPartitionColumnName = partitionColumnMeta.odpsColumnName;
        ddlBuilder.append("    `").append(odpsPartitionColumnName).append("` ").append(odpsType);

        String columnComment = partitionColumnMeta.comment;
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
    generatedStatement.setStatement(ddlBuilder.toString());

    return generatedStatement;
  }

  private void run(String inputPath, String outputPath, Mode mode) throws IOException {
    MetaManager metaManager = new MetaManager(inputPath);

    IntermediateDataManager intermediateDataDirManager =
        new IntermediateDataManager(outputPath);
    ReportBuilder reportBuilder = new ReportBuilder();

    GlobalMetaModel globalMeta = metaManager.getGlobalMeta();
    for (String databaseName : metaManager.listDatabases()) {
      DatabaseMetaModel databaseMeta = metaManager.getDatabaseMeta(databaseName);

      for (String tableName : metaManager.listTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, tableName);

        // Generate ODPS create table statements
        GeneratedStatement createTableStatement =
            getCreateTableStatement(globalMeta, databaseMeta, tableMeta);
        String formattedCreateTableStatement =
            getFormattedCreateTableStatement(databaseMeta, tableMeta, createTableStatement);
        intermediateDataDirManager.setOdpsCreateTableStatement(databaseName, tableName,
            formattedCreateTableStatement);
        reportBuilder.add(databaseName, tableName, createTableStatement);

        if (Mode.Hive.equals(mode)) {
          // Generate Hive UDTF SQL statement
          String multiPartitionHiveUdtfSql = getMultiPartitionHiveUdtfSql(databaseMeta, tableMeta);
          intermediateDataDirManager.setHiveUdtfSqlMultiPartition(
              databaseName, tableName, multiPartitionHiveUdtfSql);
        } else if (Mode.ExternalTable.equals(mode)) {
          String createExternalTableStatement = getOdpsCreateOssExternalTableStatement(globalMeta,
                                                                                       databaseMeta,
                                                                                       tableMeta);
          String wholeTableOdpsOssTransferSql = getWholeTableOdpsOssTransferSql(tableMeta);
          intermediateDataDirManager.setOdpsCreateExternalTableStatement(databaseName,
                                                                         tableName,
                                                                         createExternalTableStatement);
          intermediateDataDirManager.setOdpsOssTransferSql(databaseName,
                                                           tableName,
                                                           wholeTableOdpsOssTransferSql);
        }
      }

      for (String partitionTableName : metaManager.listPartitionTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, partitionTableName);
        TablePartitionMetaModel tablePartitionMeta =
            metaManager.getTablePartitionMeta(databaseName, partitionTableName);

        // Generate ODPS add partition statements
        Map<String, GeneratedStatement> partitionSpecToAddPartitionStatements =
            getAddPartitionStatements(databaseMeta,
                                      tableMeta,
                                      tablePartitionMeta);

        // Each add partition statement file contains less than 1000 ODPS DDLs
        List<String> addPartitionStatements = new LinkedList<>();
        for (Map.Entry<String, GeneratedStatement> entry : partitionSpecToAddPartitionStatements.entrySet()) {
          ((LinkedList<String>) addPartitionStatements).push(entry.getValue().getStatement());
        }
        intermediateDataDirManager.setOdpsAddPartitionStatement(databaseName,
                                                                partitionTableName,
                                                                addPartitionStatements);

        if (Mode.Hive.equals(mode)) {
          // Generate Hive UDTF SQL statements
          Map<String, String> partitionSpecToHiveUdtfSql =
              getSinglePartitionHiveUdtfSql(databaseMeta, tableMeta, tablePartitionMeta);

          for (String partitionSpec : partitionSpecToHiveUdtfSql.keySet()) {
            String hiveSql = partitionSpecToHiveUdtfSql.get(partitionSpec);
            intermediateDataDirManager.setHiveUdtfSqlSinglePartition(databaseName,
                                                                     partitionTableName,
                                                                     partitionSpec,
                                                                     hiveSql);
          }
        } else if (Mode.ExternalTable.equals(mode)) {
          Map<String, String> partitionSpecToAddExternalPartitionStatement =
              getOdpsAddOssExternalPartitionStatements(globalMeta,
                                                       databaseMeta,
                                                       tableMeta,
                                                       tablePartitionMeta);
          List<String> addExternalPartitionStatements = new LinkedList<>();
          for (Map.Entry<String, String> entry : partitionSpecToAddExternalPartitionStatement.entrySet()) {
            ((LinkedList<String>) addExternalPartitionStatements).push(entry.getValue());
          }
          intermediateDataDirManager.setOdpsAddExternalPartitionStatement(databaseName,
                                                                          partitionTableName,
                                                                          addExternalPartitionStatements);

          Map<String, String> partitionSpecToOdpsOssTransferSql =
              getSinglePartitionOdpsOssTransferSql(tableMeta, tablePartitionMeta);
          for (String partitionSpec : partitionSpecToOdpsOssTransferSql.keySet()) {
            String sql = partitionSpecToOdpsOssTransferSql.get(partitionSpec);
            intermediateDataDirManager.setOdpsOssTransferSqlSinglePartition(databaseName,
                                                                            partitionTableName,
                                                                            partitionSpec,
                                                                            sql);
          }
        }
      }
    }

    // Generate the report
    intermediateDataDirManager.setReport(reportBuilder.build());
  }

  private String getFormattedCreateTableStatement(DatabaseMetaModel databaseMeta,
                                                  TableMetaModel tableMeta,
                                                  GeneratedStatement generatedStatement) {
    StringBuilder builder = new StringBuilder();
    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;
    builder.append("--********************************************************************--\n")
        .append("--project name: ").append(odpsProjectName).append("\n")
        .append("--table name: ").append(odpsTableName).append("\n")
        .append("--risk level: ").append(generatedStatement.getRiskLevel()).append("\n")
        .append("--risks: \n");
    for (Risk risk : generatedStatement.getRisks()) {
      builder.append("----").append(risk.getDescription()).append("\n");
    }
    builder.append("--********************************************************************--\n");
    builder.append(generatedStatement.getStatement());
    return builder.toString();
  }

  private Map<String, GeneratedStatement> getAddPartitionStatements(
      DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta,
      TablePartitionMetaModel tablePartitionMeta) {

    Map<String, GeneratedStatement> addPartitionStatements = new HashMap<>();

    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      GeneratedStatement addPartitionStatement = new GeneratedStatement();

      String odpsProjectName = databaseMeta.odpsProjectName;
      String odpsTableName = tableMeta.odpsTableName;
      String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                      partitionMeta.partitionSpec,
                                                      false);

      String ddlBuilder = "ALTER TABLE "
                          + odpsProjectName + ".`" + odpsTableName + "` "
                          + "ADD PARTITION (" + odpsPartitionSpec + ");\n";
      addPartitionStatement.setStatement(ddlBuilder);

      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
                                                         partitionMeta.partitionSpec);
      addPartitionStatements.put(filenameSuffix, addPartitionStatement);
    }
    return addPartitionStatements;
  }

  private String getOdpsCreateOssExternalTableStatement(GlobalMetaModel globalMeta,
                                                        DatabaseMetaModel databaseMeta,
                                                        TableMetaModel tableMeta) {
    StringBuilder ddlBuilder = new StringBuilder();

    // Enable odps 2.0 data types
    ODPS_VERSION odpsVersion = ODPS_VERSION.valueOf(globalMeta.odpsVersion);
    if (ODPS_VERSION.ODPS_V2.equals(odpsVersion)) {
      ddlBuilder.append("set odps.sql.type.system.odps2=true;\n");
    }

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName + "_TMP";

    ddlBuilder
        .append("DROP TABLE IF EXISTS ")
        .append(odpsProjectName)
        .append(".`")
        .append(odpsTableName)
        .append("`;\n");

    ddlBuilder.append("CREATE EXTERNAL TABLE ");

    ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("` (\n");

    List<ColumnMetaModel> columns = tableMeta.columns;
    for (int i = 0; i < columns.size(); i++) {
      ColumnMetaModel columnMeta = columns.get(i);

      // Transform hive type to odps hive, and note down any incompatibility
      TypeTransformResult typeTransformResult = TypeTransformer.toOdpsType(globalMeta, columnMeta);
      String odpsType = typeTransformResult.getTransformedType();
      ddlBuilder.append("    `").append(columnMeta.odpsColumnName).append("` ").append(odpsType);

      if (i + 1 < columns.size()) {
        ddlBuilder.append(",\n");
      }
    }
    ddlBuilder.append(")\n");

    List<ColumnMetaModel> partitionColumns = tableMeta.partitionColumns;
    if (partitionColumns != null && partitionColumns.size() > 0) {
      ddlBuilder.append("PARTITIONED BY (\n");
      for (int i = 0; i < partitionColumns.size(); i++) {
        ColumnMetaModel partitionColumnMeta = partitionColumns.get(i);

        // Transform hive type to odps hive, and note down any incompatibility
        TypeTransformResult typeTransformResult =
            TypeTransformer.toOdpsType(globalMeta, partitionColumnMeta);
        String odpsType = typeTransformResult.getTransformedType();
        String odpsPartitionColumnName = partitionColumnMeta.odpsColumnName;
        ddlBuilder.append("    `").append(odpsPartitionColumnName).append("` ").append(odpsType);

        if (i + 1 < partitionColumns.size()) {
          ddlBuilder.append(",\n");
        }
      }
      ddlBuilder.append(")\n");
    }

    String format;
    if ("org.apache.hadoop.hive.ql.io.orc.OrcSerde".equals(tableMeta.serDe)
        && "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat".equals(tableMeta.inputFormat)
        && "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat".equals(tableMeta.outputFormat)) {
      format = "ORC";
    } else if ("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".equals(tableMeta.serDe)
               && "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".equals(tableMeta.inputFormat)
               && "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".equals(tableMeta.outputFormat)) {
      format = "PARQUET";
    } else {
      format = "TEXTFILE";
    }
    ddlBuilder.append("STORED AS ").append(format).append("\n");

    if (StringUtils.isNullOrEmpty(globalMeta.ossEndpoint)
        || StringUtils.isNullOrEmpty(globalMeta.ossBucket)) {
      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
    }
    StringBuilder locatitionBuilder = new StringBuilder();
    if (!globalMeta.ossEndpoint.startsWith("oss://")) {
      locatitionBuilder.append("oss://");
    }
    locatitionBuilder.append(globalMeta.ossEndpoint);
    if (!globalMeta.ossEndpoint.endsWith("/")) {
      locatitionBuilder.append("/");
    }
    locatitionBuilder.append(globalMeta.ossBucket);
    if (!globalMeta.ossBucket.endsWith("/")) {
      locatitionBuilder.append("/");
    }
    locatitionBuilder.append(tableMeta.tableName);

    ddlBuilder.append("LOCATION \'").append(locatitionBuilder.toString()).append("\'");
    ddlBuilder.append(";\n");

    return ddlBuilder.toString();
  }

  private Map<String, String> getOdpsAddOssExternalPartitionStatements(GlobalMetaModel globalMeta,
                                                                       DatabaseMetaModel databaseMeta,
                                                                       TableMetaModel tableMeta,
                                                                       TablePartitionMetaModel tablePartitionMeta) {
    Map<String, String> addPartitionStatements = new HashMap<>();

    ODPS_VERSION odpsVersion = ODPS_VERSION.valueOf(globalMeta.odpsVersion);
    StringBuilder settingsBuilder = new StringBuilder();
    if (ODPS_VERSION.ODPS_V2.equals(odpsVersion)) {
      settingsBuilder.append("set odps.sql.type.system.odps2=true;\n");
    }

    String settings = settingsBuilder.toString();

    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      String odpsProjectName = databaseMeta.odpsProjectName;
      String odpsTableName = tableMeta.odpsTableName + "_TMP";
      String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                      partitionMeta.partitionSpec,
                                                      false);

      if (StringUtils.isNullOrEmpty(globalMeta.ossEndpoint)
          || StringUtils.isNullOrEmpty(globalMeta.ossBucket)) {
        throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
      }
      StringBuilder locatitionBuilder = new StringBuilder();
      if (!globalMeta.ossEndpoint.startsWith("oss://")) {
        locatitionBuilder.append("oss://");
      }
      locatitionBuilder.append(globalMeta.ossEndpoint);
      if (!globalMeta.ossEndpoint.endsWith("/")) {
        locatitionBuilder.append("/");
      }
      locatitionBuilder.append(globalMeta.ossBucket);
      if (!globalMeta.ossBucket.endsWith("/")) {
        locatitionBuilder.append("/");
      }
      locatitionBuilder.append(tableMeta.tableName);
      locatitionBuilder.append("/");
      for (Map.Entry<String, String> entry : partitionMeta.partitionSpec.entrySet()) {
        locatitionBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("/");
      }

      String ddl = settings
                   + "ALTER TABLE "
                   + odpsProjectName + ".`" + odpsTableName + "` "
                   + "ADD PARTITION (" + odpsPartitionSpec + ") "
                   + "LOCATION \'" + locatitionBuilder.toString() + "\';\n";

      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
                                                         partitionMeta.partitionSpec);
      addPartitionStatements.put(filenameSuffix, ddl);
    }
    return addPartitionStatements;
  }

  private String getWholeTableOdpsOssTransferSql(TableMetaModel tableMeta) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("INSERT OVERWRITE TABLE ").append(tableMeta.odpsTableName)
        .append(" SELECT * FROM ").append(tableMeta.odpsTableName + "_TMP").append(";");
    return sqlBuilder.toString();
  }

  private Map<String, String> getSinglePartitionOdpsOssTransferSql(TableMetaModel tableMeta,
                                                                   TablePartitionMetaModel tablePartitionMeta) {
    Map<String, String> partitionSpecToOdpsTransferSql = new LinkedHashMap<>();


    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      StringBuilder sqlBuilder = new StringBuilder();
      String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                      partitionMeta.partitionSpec,
                                                      false);
      sqlBuilder
          .append("INSERT OVERWRITE TABLE ").append(tableMeta.odpsTableName)
          .append(" PARTITION(").append(odpsPartitionSpec).append(")")
          .append(" SELECT * FROM ").append(tableMeta.odpsTableName + "_TMP ")
          .append("PARTITION(").append(odpsPartitionSpec).append(");\n");

      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
                                                         partitionMeta.partitionSpec);

      partitionSpecToOdpsTransferSql.put(filenameSuffix, sqlBuilder.toString());
    }

    return partitionSpecToOdpsTransferSql;
  }

  private Map<String, String> getSinglePartitionHiveUdtfSql(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta, TablePartitionMetaModel tablePartitionMeta) {
    Map<String, String> partitionSpecToHiveSql = new LinkedHashMap<>();

    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      String odpsTableName = tableMeta.odpsTableName;
      List<String> hiveColumnNames = new ArrayList<>();
      List<String> odpsColumnNames = new ArrayList<>();
      for (ColumnMetaModel columnMeta : tableMeta.columns) {
        odpsColumnNames.add(columnMeta.odpsColumnName);
        hiveColumnNames.add(columnMeta.columnName);
      }
      String hivePartitionSpec = getHivePartitionSpec(tableMeta.partitionColumns,
                                                      partitionMeta.partitionSpec,
                                                      false);
      String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                      partitionMeta.partitionSpec,
                                                      true);

      StringBuilder hiveUdtfSqlBuilder = new StringBuilder();
      hiveUdtfSqlBuilder.append("SELECT odps_data_dump_single(\n")
          .append("\'").append(databaseMeta.odpsProjectName).append("\',\n")
          .append("\'").append(odpsTableName).append("\',\n")
          .append("\'").append(odpsPartitionSpec).append("\',\n")
          .append("\'").append(String.join(",", odpsColumnNames)).append("\',\n");
      for (int i = 0; i < hiveColumnNames.size(); i++) {
        hiveUdtfSqlBuilder.append("`").append(hiveColumnNames.get(i)).append("`");
        if (i != hiveColumnNames.size() - 1) {
          hiveUdtfSqlBuilder.append(",\n");
        } else {
          hiveUdtfSqlBuilder.append(")\n");
        }
      }
      hiveUdtfSqlBuilder.append("FROM ")
          .append(databaseMeta.databaseName).append(".`").append(tableMeta.tableName).append("`\n")
          .append("WHERE ").append(hivePartitionSpec).append(";\n");

      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
                                                         partitionMeta.partitionSpec);
      partitionSpecToHiveSql.put(filenameSuffix, hiveUdtfSqlBuilder.toString());
    }

    return partitionSpecToHiveSql;
  }

//  private Map<String, String> getSinglePartitionHiveVerifySql(
//      DatabaseMetaModel databaseMeta,
//      TableMetaModel tableMeta,
//      TablePartitionMetaModel tablePartitionMeta) {
//    Map<String, String> partitionSpecToHiveSql = new HashMap<>();
//
//    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
//      String hivePartitionSpec = getHivePartitionSpec(tableMeta.partitionColumns,
//                                                      partitionMeta.partitionSpec,
//                                                      false);
//
//      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
//                                                         partitionMeta.partitionSpec);
//      String hiveSqlBuilder = "SELECT count(*) "
//                              + "FROM "
//                              + databaseMeta.databaseName + ".`" + tableMeta.tableName + "` "
//                              + "WHERE " + hivePartitionSpec + ";\n";
//      partitionSpecToHiveSql.put(filenameSuffix, hiveSqlBuilder);
//    }
//
//    return partitionSpecToHiveSql;
//  }

//  private String getWholeTableHiveVerifySql(DatabaseMetaModel databaseMeta,
//                                            TableMetaModel tableMeta) {
//    return "SELECT count(*) "
//           + "FROM "
//           + databaseMeta.databaseName + ".`" + tableMeta.tableName + "`" + ";\n";
//  }

//  private Map<String, String> getSinglePartitionOdpsVerifySql(
//      DatabaseMetaModel databaseMeta,
//      TableMetaModel tableMeta,
//      TablePartitionMetaModel tablePartitionMeta) {
//    Map<String, String> partitionSpecToOdpsSql = new HashMap<>();
//
//    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
//      String odpsPartitionSpec = getOdpsPartitionSpecForQuery(tableMeta.partitionColumns,
//                                                      partitionMeta.partitionSpec,
//                                                      false);
//
//      String filenameSuffix = getPartitionSpecAsFilename(tableMeta.partitionColumns,
//                                                         partitionMeta.partitionSpec);
//      String hiveSqlBuilder = "SELECT count(*) "
//                              + "FROM "
//                              + databaseMeta.odpsProjectName + ".`" + tableMeta.odpsTableName + "` "
//                              + "WHERE " + odpsPartitionSpec + ";\n";
//      partitionSpecToOdpsSql.put(filenameSuffix, hiveSqlBuilder);
//    }
//
//    return partitionSpecToOdpsSql;
//  }

//  private String getWholeTableOdpsVerifySql(DatabaseMetaModel databaseMeta,
//                                            TableMetaModel tableMeta) {
//    String settings = "SET odps.sql.allow.fullscan=true;\n";
//    String query = "SELECT count(*) "
//                   + "FROM "
//                   + databaseMeta.odpsProjectName + ".`" + tableMeta.odpsTableName + "`" + ";\n";
//    return settings + query;
//  }

  private String getHivePartitionSpec(List<ColumnMetaModel> partitionColumns,
      Map<String, String> hivePartitionSpec, boolean escape) {
    StringBuilder hivePartitionSpecBuilder = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = hivePartitionSpec.get(partitionColumn.columnName);

      hivePartitionSpecBuilder
          .append(partitionColumn.columnName)
          .append("=");
      if (escape) {
        hivePartitionSpecBuilder.append("\\\'").append(partitionValue).append("\\\'");
      } else {
        hivePartitionSpecBuilder.append("\'").append(partitionValue).append("\'");
      }

      if (i != partitionColumns.size() - 1) {
        hivePartitionSpecBuilder.append(" AND ");
      }
    }

    return hivePartitionSpecBuilder.toString();
  }

  private String getOdpsPartitionSpec(List<ColumnMetaModel> partitionColumns,
                                      Map<String, String> hivePartitionSpec,
                                      boolean escape) {
    StringBuilder odpsPartitionSpecBuilder = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = hivePartitionSpec.get(partitionColumn.columnName);

      odpsPartitionSpecBuilder
          .append(partitionColumn.odpsColumnName)
          .append("=");
      if (escape) {
        odpsPartitionSpecBuilder.append("\\\'").append(partitionValue).append("\\\'");
      } else {
        odpsPartitionSpecBuilder.append("\'").append(partitionValue).append("\'");
      }
      if (i != partitionColumns.size() - 1) {
        odpsPartitionSpecBuilder.append(",");
      }
    }

    return odpsPartitionSpecBuilder.toString();
  }

  private String getOdpsPartitionSpecForQuery(List<ColumnMetaModel> partitionColumns,
                                              Map<String, String> hivePartitionSpec,
                                              boolean escape) {
    StringBuilder odpsPartitionSpecBuilder = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = hivePartitionSpec.get(partitionColumn.columnName);

      odpsPartitionSpecBuilder
          .append(partitionColumn.odpsColumnName)
          .append("=");
      if (escape) {
        odpsPartitionSpecBuilder.append("\\\'").append(partitionValue).append("\\\'");
      } else {
        odpsPartitionSpecBuilder.append("\'").append(partitionValue).append("\'");
      }
      if (i != partitionColumns.size() - 1) {
        odpsPartitionSpecBuilder.append(" AND ");
      }
    }

    return odpsPartitionSpecBuilder.toString();
  }

  private String getPartitionSpecAsFilename(List<ColumnMetaModel> partitionColumns,
                                            Map<String, String> hivePartitionSpec) {
    StringBuilder filenameBuilder = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = hivePartitionSpec.get(partitionColumn.columnName);

      filenameBuilder
          .append(partitionColumn.odpsColumnName)
          .append("_")
          .append(partitionValue);
      if (i != partitionColumns.size() - 1) {
        filenameBuilder.append(",");
      }
    }

    return filenameBuilder.toString();
  }

  private String getMultiPartitionHiveUdtfSql(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta) {
    StringBuilder hiveUdtfSqlBuilder = new StringBuilder();

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
    hiveUdtfSqlBuilder.append("SELECT odps_data_dump_multi(\n")
        .append("\'").append(databaseMeta.odpsProjectName).append("\',\n")
        .append("\'").append(tableMeta.odpsTableName).append("\',\n")
        .append("\'").append(String.join(",", odpsColumnNames)).append("\',\n")
        .append("\'").append(String.join(",", odpsPartitionColumnNames)).append("\',\n");
    for (int i = 0; i < hiveColumnNames.size(); i++) {
      hiveUdtfSqlBuilder.append("`").append(hiveColumnNames.get(i)).append("`");
      if (i != hiveColumnNames.size() - 1) {
        hiveUdtfSqlBuilder.append(",\n");
      } else {
        hiveUdtfSqlBuilder.append(")\n");
      }
    }
    String databaseName = databaseMeta.databaseName;
    String tableName = tableMeta.tableName;
    hiveUdtfSqlBuilder.append("FROM ")
        .append(databaseName).append(".`").append(tableName).append("`").append(";\n");

    return hiveUdtfSqlBuilder.toString();
  }

  public static void main(String[] args) throws Exception {
    Option meta = Option
        .builder("i")
        .longOpt("input-dir")
        .argName("input-dir")
        .hasArg()
        .desc("Directory generated by meta carrier")
        .build();
    Option outputDir = Option
        .builder("o")
        .longOpt("output-dir")
        .argName("output-dir")
        .hasArg()
        .desc("Output directory generated by meta processor")
        .build();
    Option mode = Option
        .builder("m")
        .longOpt("mode")
        .argName("mode")
        .hasArg()
        .desc("Mode, available choices: Hive, ExternalTable")
        .build();
    Option help = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .desc("Print help information")
        .build();

    Options options = new Options();
    options.addOption(meta);
    options.addOption(outputDir);
    options.addOption(mode);
    options.addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    Mode modeValue = Mode.valueOf(cmd.getOptionValue("mode", "Hive"));

    if (cmd.hasOption("input-dir") && cmd.hasOption("output-dir") && !cmd.hasOption("help")) {
      MetaProcessor metaProcessor = new MetaProcessor();
      metaProcessor.run(cmd.getOptionValue("input-dir"),
                        cmd.getOptionValue("output-dir"),
                        modeValue);
    } else {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
            "meta-processor -i <metadata directory> -o <output directory> -m mode [Hive|ExternalTable]";
        formatter.printHelp(cmdLineSyntax, options);
    }
  }
}
