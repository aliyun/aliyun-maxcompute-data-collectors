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

package com.aliyun.odps.datacarrier.metacarrier;

import com.aliyun.odps.datacarrier.commons.MetaManager;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.DatabaseMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;
import java.io.IOException;
import java.util.List;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class HiveMetaCarrier {
  private HiveMetaStoreClient metaStoreClient;
  private MetaManager metaManager;

  public HiveMetaCarrier(String metastoreAddress, String outputPath) throws MetaException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTOREURIS, metastoreAddress);

    this.metaStoreClient = new HiveMetaStoreClient(hiveConf);
    this.metaManager = new MetaManager(outputPath);
  }

  private GlobalMetaModel getGlobalMeta() {
    GlobalMetaModel globalMetaModel = new GlobalMetaModel();
    globalMetaModel.datasourceType = "HIVE";
    return globalMetaModel;
  }

  private DatabaseMetaModel getDatabaseMeta(String databaseName) {
    DatabaseMetaModel databaseMeta = new DatabaseMetaModel();
    databaseMeta.databaseName = databaseName;
    databaseMeta.odpsProjectName = databaseName;
    return databaseMeta;
  }

  private TableMetaModel getTableMeta(String databaseName, String tableName) throws TException {
    Table table = metaStoreClient.getTable(databaseName, tableName);
    TableMetaModel tableMeta = new TableMetaModel();
    tableMeta.tableName = tableName;
    tableMeta.odpsTableName = tableName;
    List<FieldSchema> columns = metaStoreClient.getFields(databaseName, tableName);
    for (FieldSchema column : columns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = column.getName();
      columnMetaModel.odpsColumnName = column.getName();
      columnMetaModel.type = column.getType();
      columnMetaModel.comment = column.getComment();
      tableMeta.columns.add(columnMetaModel);
    }
    List<FieldSchema> partitionColumns = table.getPartitionKeys();
    for (FieldSchema partitionColumn : partitionColumns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = partitionColumn.getName();
      columnMetaModel.odpsColumnName = partitionColumn.getName();
      columnMetaModel.type = partitionColumn.getType();
      columnMetaModel.comment = partitionColumn.getComment();
      tableMeta.partitionColumns.add(columnMetaModel);
    }
    return tableMeta;
  }

  private TablePartitionMetaModel getTablePartitionMeta(String databaseName, String tableName)
      throws TException {
    List<Partition> partitions =
        metaStoreClient.listPartitions(databaseName, tableName, Short.MAX_VALUE);
    if (partitions.isEmpty()) {
      return null;
    }

    TablePartitionMetaModel tablePartitionMeta = new TablePartitionMetaModel();
    Table table = metaStoreClient.getTable(databaseName, tableName);
    List<FieldSchema> partitionColumns = table.getPartitionKeys();

    tablePartitionMeta.tableName = tableName;
    for (Partition partition : partitions) {
      PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
      partitionMetaModel.createTime = Integer.toString(partition.getCreateTime());
      partitionMetaModel.location = partition.getSd().getLocation();
      // Generate partition specifications
      List<String> partitionValues = partition.getValues();
      for (int i = 0; i < partitionColumns.size(); i++) {
        partitionMetaModel.partitionSpec.put(
            partitionColumns.get(i).getName(), partitionValues.get(i));
      }
      tablePartitionMeta.partitions.add(partitionMetaModel);
    }

    return tablePartitionMeta;
  }

  public ProgressBar initProgressBar(long max) {
    ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
    progressBarBuilder.setInitialMax(max);
    progressBarBuilder.setStyle(ProgressBarStyle.ASCII);
    return progressBarBuilder.build();
  }

  public void carrySingleTableMeta(String databaseName, String tableName)
      throws IOException, TException {
    System.out.println("Working on " + databaseName + "." + tableName);
    GlobalMetaModel globalMeta = getGlobalMeta();
    metaManager.setGlobalMeta(globalMeta);

    DatabaseMetaModel databaseMeta = getDatabaseMeta(databaseName);
    metaManager.setDatabaseMeta(databaseMeta);

    TableMetaModel tableMeta = getTableMeta(databaseName, tableName);
    metaManager.setTableMeta(databaseName, tableMeta);

    TablePartitionMetaModel tablePartitionMeta =
        getTablePartitionMeta(databaseName, tableName);
    if (tablePartitionMeta != null) {
      metaManager.setTablePartitionMeta(databaseName, tablePartitionMeta);
    }
  }

  public void carrySingleDatabase(String databaseName) throws IOException, TException {
    GlobalMetaModel globalMeta = getGlobalMeta();
    metaManager.setGlobalMeta(globalMeta);

    List<String> tableNames = metaStoreClient.getAllTables(databaseName);
    DatabaseMetaModel databaseMeta = getDatabaseMeta(databaseName);
    metaManager.setDatabaseMeta(databaseMeta);

    // Create progress bar for this database
    System.out.println("Working on " + databaseName);
    ProgressBar progressBar = initProgressBar(tableNames.size());

    // Iterate over tables
    for (String tableName :  tableNames) {
      // Update progress bar
      progressBar.step();
      progressBar.setExtraMessage("Working on " + databaseName + "." + tableName);

      TableMetaModel tableMeta = getTableMeta(databaseName, tableName);
      metaManager.setTableMeta(databaseName, tableMeta);

      // Handle partition meta
      TablePartitionMetaModel tablePartitionMeta =
          getTablePartitionMeta(databaseName, tableName);
      if (tablePartitionMeta != null) {
        metaManager.setTablePartitionMeta(databaseName, tablePartitionMeta);
      }
    }
    progressBar.close();
  }

  public void carryAll() throws IOException, TException {
    GlobalMetaModel globalMeta = getGlobalMeta();
    metaManager.setGlobalMeta(globalMeta);

    List<String> databaseNames = metaStoreClient.getAllDatabases();

    // Iterate over databases
    for (String databaseName : databaseNames) {
      List<String> tableNames = metaStoreClient.getAllTables(databaseName);
      DatabaseMetaModel databaseMeta = getDatabaseMeta(databaseName);
      metaManager.setDatabaseMeta(databaseMeta);

      // Create progress bar for this database
      System.out.println("Working on " + databaseName);
      ProgressBar progressBar = initProgressBar(tableNames.size());

      // Iterate over tables
      for (String tableName :  tableNames) {
        // Update progress bar
        progressBar.step();
        progressBar.setExtraMessage("Working on " + databaseName + "." + tableName);

        TableMetaModel tableMeta = getTableMeta(databaseName, tableName);
        metaManager.setTableMeta(databaseName, tableMeta);

        // Handle partition meta
        TablePartitionMetaModel tablePartitionMeta =
            getTablePartitionMeta(databaseName, tableName);
        if (tablePartitionMeta != null) {
          metaManager.setTablePartitionMeta(databaseName, tablePartitionMeta);
        }
      }
      progressBar.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Option uri = Option
        .builder("u")
        .longOpt("uri")
        .argName("uri")
        .hasArg()
        .desc("hive metastore thrift uri")
        .build();
    Option outputDir = Option
        .builder("o")
        .longOpt("output-dir")
        .argName("output-dir")
        .hasArg()
        .desc("Output directory")
        .build();
    Option database = Option
        .builder("d")
        .longOpt("database")
        .argName("database")
        .hasArg()
        .desc("Specify a database")
        .build();
    Option table = Option
        .builder("t")
        .longOpt("table")
        .argName("table")
        .hasArg()
        .desc("Specify a table")
        .build();
    Option help = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .desc("Print help information")
        .build();

    Options options = new Options();
    options.addOption(uri);
    options.addOption(outputDir);
    options.addOption(help);
    options.addOption(database);
    options.addOption(table);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (!cmd.hasOption("uri") || !cmd.hasOption("output-dir") || cmd.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(
          "meta-carrier -u <uri> -o <output dir> [-h] [-d <database>] [-t <table>]",
          options);
    } else{
      String hiveMetastoreAddress = cmd.getOptionValue("uri");
      String outputPath = cmd.getOptionValue("output-dir");
      HiveMetaCarrier hiveMetaCarrier = new HiveMetaCarrier(hiveMetastoreAddress, outputPath);
      if (cmd.hasOption("database") && cmd.hasOption("table")) {
        String databaseName = cmd.getOptionValue("database");
        String tableName = cmd.getOptionValue("table");
        hiveMetaCarrier.carrySingleTableMeta(databaseName, tableName);
      } else if (cmd.hasOption("database")) {
        String databaseName = cmd.getOptionValue("database");
        hiveMetaCarrier.carrySingleDatabase(databaseName);
      } else {
        hiveMetaCarrier.carryAll();
      }
    }
  }
}
