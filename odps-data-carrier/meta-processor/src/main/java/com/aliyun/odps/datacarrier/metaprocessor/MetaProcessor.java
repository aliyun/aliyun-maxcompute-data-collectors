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
import java.util.Iterator;
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

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;

    // Register to name manager, check if there is any conflict
    Risk risk = nameManager.add(databaseMeta.databaseName,
                                odpsProjectName,
                                tableMeta.tableName,
                                odpsTableName);
    generatedStatement.setRisk(risk);

    // Enable odps 2.0 data types
    ddlBuilder.append("SET odps.sql.type.system.odps2=true;\n");

    if (databaseMeta.dropTableIfExists || tableMeta.dropIfExists) {
      ddlBuilder
          .append("DROP TABLE IF EXISTS ")
          .append(odpsProjectName).append(".`").append(odpsTableName).append("`;\n");
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

  private List<String> getAddPartitionStatements(DatabaseMetaModel databaseMeta,
                                                             TableMetaModel tableMeta,
                                                             TablePartitionMetaModel tablePartitionMeta) {
    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;

    List<String> addPartitionStatements = new LinkedList<>();

    Iterator<PartitionMetaModel> iterator = tablePartitionMeta.partitions.iterator();
    while (iterator.hasNext()) {
      StringBuilder ddlBuilder = new StringBuilder();
      ddlBuilder.append("SET odps.sql.type.system.odps2=true;\n");
      ddlBuilder.append("ALTER TABLE\n");
      ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("`\n");
      ddlBuilder.append("ADD");

      for (int i = 0; i < 1000; i++) {
        if (iterator.hasNext()) {
          PartitionMetaModel partitionMeta = iterator.next();
          String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                          partitionMeta.partitionSpec,
                                                          false);
          ddlBuilder.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
        } else {
          break;
        }
      }
      ddlBuilder.append(";\n");
      addPartitionStatements.add(ddlBuilder.toString());
    }

    return addPartitionStatements;
  }

  private String getOdpsCreateOssExternalTableStatement(GlobalMetaModel globalMeta,
                                                        DatabaseMetaModel databaseMeta,
                                                        TableMetaModel tableMeta) {
    StringBuilder ddlBuilder = new StringBuilder();

    // Enable odps 2.0 data types
    ddlBuilder.append("set odps.sql.type.system.odps2=true;\n");

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

    ddlBuilder
        .append("ROW FORMAT SERDE\n")
        .append("\'").append(tableMeta.serDe).append("\'\n")
        .append("STORED AS INPUTFORMAT\n")
        .append("\'").append(tableMeta.inputFormat).append("\'\n")
        .append("OUTPUTFORMAT\n")
        .append("\'").append(tableMeta.outputFormat).append("\'\n");

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
    ddlBuilder.append("LOCATION \'").append(locatitionBuilder.toString()).append("\';\n");

    return ddlBuilder.toString();
  }

  private List<String> getOdpsAddOssExternalPartitionStatements(GlobalMetaModel globalMeta,
                                                                DatabaseMetaModel databaseMeta,
                                                                TableMetaModel tableMeta,
                                                                TablePartitionMetaModel tablePartitionMeta) {
    if (StringUtils.isNullOrEmpty(globalMeta.ossEndpoint)
        || StringUtils.isNullOrEmpty(globalMeta.ossBucket)) {
      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
    }

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName + "_TMP";

    List<String> addPartitionStatements = new LinkedList<>();

    Iterator<PartitionMetaModel> iterator = tablePartitionMeta.partitions.iterator();
    while (iterator.hasNext()) {
      StringBuilder ddlBuilder = new StringBuilder();
      ddlBuilder.append("SET odps.sql.type.system.odps2=true;\n");
      ddlBuilder.append("ALTER TABLE\n");
      ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("`\n");
      ddlBuilder.append("ADD");

      for (int i = 0; i < 1000; i++) {
        if (iterator.hasNext()) {
          PartitionMetaModel partitionMeta = iterator.next();

          String odpsPartitionSpec = getOdpsPartitionSpec(tableMeta.partitionColumns,
                                                          partitionMeta.partitionSpec,
                                                          false);

          StringBuilder locationBuilder = new StringBuilder();
          if (!globalMeta.ossEndpoint.startsWith("oss://")) {
            locationBuilder.append("oss://");
          }
          locationBuilder.append(globalMeta.ossEndpoint);
          if (!globalMeta.ossEndpoint.endsWith("/")) {
            locationBuilder.append("/");
          }
          locationBuilder.append(globalMeta.ossBucket);
          if (!globalMeta.ossBucket.endsWith("/")) {
            locationBuilder.append("/");
          }
          locationBuilder.append(databaseMeta.databaseName).append("/");
          locationBuilder.append(tableMeta.tableName).append("/");
          for (Map.Entry<String, String> entry : partitionMeta.partitionSpec.entrySet()) {
            locationBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append("/");
          }

          ddlBuilder.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
          ddlBuilder.append("\nLOCATION \'").append(locationBuilder.toString()).append("\'");
        } else {
          break;
        }
      }
      ddlBuilder.append(";\n");
      addPartitionStatements.add(ddlBuilder.toString());
    }
    return addPartitionStatements;
  }

  private String getOdpsOssTransferSql(TableMetaModel tableMeta) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("SET odps.sql.allow.fullscan=true; ");
    sqlBuilder.append("INSERT OVERWRITE TABLE ").append(tableMeta.odpsTableName);

    if (tableMeta.partitionColumns.size() != 0) {
      sqlBuilder.append(" PARTITION(");
      for (int i = 0; i < tableMeta.partitionColumns.size(); i++) {
        sqlBuilder.append(tableMeta.partitionColumns.get(i).columnName);
        if (i != tableMeta.partitionColumns.size() - 1) {
          sqlBuilder.append(", ");
        }
      }
      sqlBuilder.append(")");
    }
    sqlBuilder.append(" SELECT * FROM ").append(tableMeta.odpsTableName).append("_TMP; ");
    sqlBuilder.append("DROP TABLE IF EXISTS ").append(tableMeta.odpsTableName).append("_TMP; ");
    return sqlBuilder.toString();
  }

  private String getMultiPartitionHiveUdtfSql(DatabaseMetaModel databaseMeta,
                                              TableMetaModel tableMeta,
                                              int version) {
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
        .append(databaseName).append(".`").append(tableName).append("`");

    //Use Hive JDBC, sql statements cannot end with ';'
    if (version == 1) {
      hiveUdtfSqlBuilder.append(";\n");
    }

    return hiveUdtfSqlBuilder.toString();
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

  private void run(String inputPath, String outputPath, int version) throws IOException {
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

        // Generate Hive UDTF SQL statement
        String multiPartitionHiveUdtfSql = getMultiPartitionHiveUdtfSql(databaseMeta, tableMeta, version);
        intermediateDataDirManager.setHiveUdtfSqlMultiPartition(
            databaseName, tableName, multiPartitionHiveUdtfSql);

        // Generate ODPS create external table statements & data transfer SQL
        String createExternalTableStatement = getOdpsCreateOssExternalTableStatement(globalMeta,
                                                                                     databaseMeta,
                                                                                     tableMeta);
        intermediateDataDirManager.setOdpsCreateExternalTableStatement(databaseName,
                                                                       tableName,
                                                                       createExternalTableStatement);
        String odpsOssTransferSql = getOdpsOssTransferSql(tableMeta);
        intermediateDataDirManager.setOdpsOssTransferSql(databaseName,
                                                         tableName,
                                                         odpsOssTransferSql);
      }

      for (String partitionTableName : metaManager.listPartitionTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, partitionTableName);
        TablePartitionMetaModel tablePartitionMeta =
            metaManager.getTablePartitionMeta(databaseName, partitionTableName);

        // Generate ODPS add partition statements
        List<String> addPartitionStatements = getAddPartitionStatements(databaseMeta,
                                                                        tableMeta,
                                                                        tablePartitionMeta);

        // Each add partition statement file contains less than 1000 ODPS DDLs
        intermediateDataDirManager.setOdpsAddPartitionStatement(databaseName,
                                                                partitionTableName,
                                                                addPartitionStatements);

        // Generate ODPS add external partition statements & data transfer SQL
        List<String> addExternalPartitionStatements =
            getOdpsAddOssExternalPartitionStatements(globalMeta,
                                                     databaseMeta,
                                                     tableMeta,
                                                     tablePartitionMeta);
        intermediateDataDirManager.setOdpsAddExternalPartitionStatement(databaseName,
                                                                        partitionTableName,
                                                                        addExternalPartitionStatements);
      }
    }

    // Generate the report
    intermediateDataDirManager.setReport(reportBuilder.build());
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
    Option version = Option
        .builder("v")
        .longOpt("version")
        .argName("version")
        .hasArg()
        .desc("Migration task version, 1: ODPS Cmd / Hive Cli; 2: ODPS SDK / Hive JDBC")
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
    options.addOption(version);
    options.addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("input-dir") && cmd.hasOption("output-dir") && cmd.hasOption("version")
        && !cmd.hasOption("help")) {
      MetaProcessor metaProcessor = new MetaProcessor();
      metaProcessor.run(cmd.getOptionValue("input-dir"),
          cmd.getOptionValue("output-dir"),
          Integer.valueOf(cmd.getOptionValue("version")));
    } else {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
            "meta-processor -i <metadata directory> -o <output directory> -m mode [Hive|ExternalTable]";
        formatter.printHelp(cmdLineSyntax, options);
    }
  }
}
