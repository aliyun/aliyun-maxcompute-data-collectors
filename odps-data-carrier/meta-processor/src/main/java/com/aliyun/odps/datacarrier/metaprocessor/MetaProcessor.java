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
import java.io.IOException;
import java.util.ArrayList;
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
 *
 * usage:
 * java -cp /path/to/jar com.aliyun.odps.datacarrier.odps.MetaProcessor [meta directory path]
 * [result directory path]
 *
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
      DatabaseMetaModel databaseMeta, TableMetaModel tableMeta) {
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

    ddlBuilder.append("CREATE TABLE ");

    if (tableMeta.ifNotExists) {
      ddlBuilder.append(" IF NOT EXISTS ");
    }

    String odpsProjectName = databaseMeta.odpsProjectName;
    String odpsTableName = tableMeta.odpsTableName;
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

  private void run(String inputPath, String outputPath) throws IOException {
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

        // Generate Hive UDTF SQL statements
        String multiPartitionHiveUdtfSQL = getMultiPartitionHiveUdtfSQL(databaseMeta, tableMeta);
        intermediateDataDirManager.setHiveUdtfSQLMultiPartition(
            databaseName, tableName, multiPartitionHiveUdtfSQL);
      }

      for (String partitionTableName : metaManager.listPartitionTables(databaseName)) {
        TableMetaModel tableMeta = metaManager.getTableMeta(databaseName, partitionTableName);
        TablePartitionMetaModel tablePartitionMeta =
            metaManager.getTablePartitionMeta(databaseName, partitionTableName);

        // Generate ODPS add partition statements
        List<GeneratedStatement> createPartitionStatements =
            getCreatePartitionStatements(globalMeta, databaseMeta, tableMeta, tablePartitionMeta);
        StringBuilder contentBuilder = new StringBuilder();
        for (GeneratedStatement generatedStatement : createPartitionStatements) {
          contentBuilder.append(generatedStatement.getStatement()).append("\n");
        }
        intermediateDataDirManager.setOdpsAddPartitionStatement(databaseName, partitionTableName,
            contentBuilder.toString());

        // Generate Hive UDTF SQL statements
        List<String> singlePartitionHiveUdtfSQL =
            getSinglePartitionHiveUdtfSQL(databaseMeta, tableMeta, tablePartitionMeta);
        intermediateDataDirManager.setHiveUdtfSQLSinglePartition(databaseName, partitionTableName,
            String.join("\n", singlePartitionHiveUdtfSQL));
      }
    }

    // Generate the report
    intermediateDataDirManager.setReport(reportBuilder.build());
  }

  private String getFormattedCreateTableStatement(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta, GeneratedStatement generatedStatement) {
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

  private List<GeneratedStatement> getCreatePartitionStatements(GlobalMetaModel globalMeta,
      DatabaseMetaModel databaseMeta, TableMetaModel tableMeta,
      TablePartitionMetaModel tablePartitionMeta) {
    List<GeneratedStatement> createPartitionStatements = new ArrayList<>();

    ODPS_VERSION odpsVersion = ODPS_VERSION.valueOf(globalMeta.odpsVersion);
    if (ODPS_VERSION.ODPS_V2.equals(odpsVersion)) {
      GeneratedStatement setStatement = new GeneratedStatement();
      setStatement.setStatement("set odps.sql.type.system.odps2=true;\n");
      createPartitionStatements.add(setStatement);
    }

    for (PartitionMetaModel partitionMeta : tablePartitionMeta.partitions) {
      GeneratedStatement createPartitionStatement = new GeneratedStatement();
      StringBuilder ddlBuilder = new StringBuilder();
      ddlBuilder.append("ALTER TABLE ");

      String odpsProjectName = databaseMeta.odpsProjectName;
      String odpsTableName = tableMeta.odpsTableName;
      ddlBuilder.append(odpsProjectName).append(".`").append(odpsTableName).append("` ");
      ddlBuilder.append("ADD PARTITION (").append(partitionMeta.partitionSpec).append(");\n");

      createPartitionStatement.setStatement(ddlBuilder.toString());
      createPartitionStatements.add(createPartitionStatement);
    }
    return createPartitionStatements;
  }

  private List<String> getSinglePartitionHiveUdtfSQL(DatabaseMetaModel databaseMeta,
      TableMetaModel tableMeta, TablePartitionMetaModel tablePartitionMeta) {
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
      hiveUdtfSQLBuilder.append("SELECT odps_data_dump_single(\n")
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
      hiveUdtfSQLBuilder.append("FROM ")
          .append(databaseMeta.databaseName).append(".`").append(tableMeta.tableName).append("`\n")
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
    hiveUdtfSQLBuilder.append("SELECT odps_data_dump_multi(\n")
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
    hiveUdtfSQLBuilder.append("FROM ")
        .append(databaseName).append(".`").append(tableName).append("`").append(";\n");

    return hiveUdtfSQLBuilder.toString();
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
    Option help = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .desc("Print help information")
        .build();

    Options options = new Options();
    options.addOption(meta);
    options.addOption(outputDir);
    options.addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("input-dir") && cmd.hasOption("output-dir") && !cmd.hasOption("help")) {
      MetaProcessor metaProcessor = new MetaProcessor();
      metaProcessor.run(cmd.getOptionValue("input-dir"), cmd.getOptionValue("output-dir"));
    } else {
        HelpFormatter formatter = new HelpFormatter();
        String cmdLineSyntax =
            "meta-processor -i <metadata directory> -o <output directory>";
        formatter.printHelp(cmdLineSyntax, options);
    }
  }
}
