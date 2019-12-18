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

import java.io.IOException;
import java.util.List;

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

import com.aliyun.odps.datacarrier.commons.MetaManager;
import com.aliyun.odps.datacarrier.commons.MetaManager.ColumnMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.DatabaseMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.GlobalMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.PartitionMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class HiveMetaCarrier {
  private HiveMetaStoreClient metaStoreClient;
  private MetaManager metaManager;

  public HiveMetaCarrier(String metastoreAddress, String outputPath, String principal,
      String keyTab, String[] systemProperties) throws MetaException {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTOREURIS, metastoreAddress);
    if (principal != null) {
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
      hiveConf.setVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
    }
    if (keyTab != null) {
      hiveConf.setVar(ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTab);
    }
    if (systemProperties != null) {
      for (String property : systemProperties) {
        int idx = property.indexOf('=');
        if (idx != -1) {
          System.setProperty(property.substring(0, idx), property.substring(idx + 1));
        } else {
          System.err.print("Invalid system property: " + property);
        }
      }
    }

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
    tableMeta.location = table.getSd().getLocation();
    tableMeta.inputFormat = table.getSd().getInputFormat();
    tableMeta.outputFormat = table.getSd().getOutputFormat();
    tableMeta.serDe = table.getSd().getSerdeInfo().getSerializationLib();
    tableMeta.serDeProperties = table.getSd().getSerdeInfo().getParameters();
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
        metaStoreClient.listPartitions(databaseName, tableName, (short) -1);
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

  public void carry(MetaCarrierConfiguration configuration) throws IOException, TException {
    GlobalMetaModel globalMeta = getGlobalMeta();
    metaManager.setGlobalMeta(globalMeta);

    List<String> databaseNames = metaStoreClient.getAllDatabases();
    for (String databaseName : databaseNames) {
      if (!configuration.shouldCarry(databaseName)) {
        continue;
      }
      DatabaseMetaModel databaseMeta = getDatabaseMeta(databaseName);
      metaManager.setDatabaseMeta(databaseMeta);

      List<String> tableNames = metaStoreClient.getAllTables(databaseName);
      // Create progress bar for this database
      System.err.println("Working on " + databaseName);
      ProgressBar progressBar = initProgressBar(tableNames.size());

      for (String tableName :  tableNames) {
        // Update progress bar
        progressBar.step();
        progressBar.setExtraMessage("Working on " + databaseName + "." + tableName);

        if (!configuration.shouldCarry(databaseName, tableName)) {
          continue;
        }

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

  private static void validateCommandLine(CommandLine commandLine, Options options) {
    if (!commandLine.hasOption("uri")) {
      System.err.println("Please provide the thrift address of Hive MetaStore Service");
    }
    if (!commandLine.hasOption("output-dir")) {
      System.err.println("Please provide an output directory");
    }

    if (commandLine.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("sh meta-carrier", options);
      System.exit(0);
    }
  }

  public static void main(String[] args) throws Exception {
    Option uri = Option
        .builder("u")
        .longOpt("uri")
        .argName("uri")
        .hasArg()
        .required()
        .desc("Required, hive metastore thrift uri")
        .build();
    Option outputDir = Option
        .builder("o")
        .longOpt("output-dir")
        .argName("output-dir")
        .hasArg()
        .required()
        .desc("Required, output directory")
        .build();
    Option databases = Option
        .builder("d")
        .longOpt("databases")
        .argName("databases")
        .hasArgs()
        .desc("Optional, specify databases to migrate")
        .build();
    Option tables = Option
        .builder("t")
        .longOpt("tables")
        .argName("tables")
        .hasArgs()
        .desc("Optional, specify tables to migrate. The format should be: <hive db>.<hive table>")
        .build();
    Option configPath = Option
        .builder()
        .longOpt("config")
        .argName("config")
        .hasArg()
        .desc("Optional, specify tables to migrate. "
              + "Each line should be in the following format: <hive db>.<hive table>")
        .build();
    Option help = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .desc("Optional, print help information")
        .build();
    Option principal = Option
        .builder()
        .longOpt("principal")
        .argName("principal")
        .hasArg()
        .desc("Optional, hive metastore's Kerberos principal")
        .build();
    Option keyTab = Option
        .builder()
        .longOpt("keyTab")
        .argName("keyTab")
        .hasArg()
        .desc("Optional, hive metastore's Kerberos keyTab")
        .build();
    Option systemProperties = Option
        .builder()
        .longOpt("system")
        .argName("system")
        .hasArg()
        .numberOfArgs(Option.UNLIMITED_VALUES)
        .desc("system properties")
        .build();

    Options options = new Options();
    options.addOption(uri);
    options.addOption(outputDir);
    options.addOption(help);
    options.addOption(databases);
    options.addOption(tables);
    options.addOption(configPath);
    options.addOption(principal);
    options.addOption(keyTab);
    options.addOption(systemProperties);

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("sh meta-carrier", options);
      return;
    }

    validateCommandLine(commandLine, options);

    String uriValue = commandLine.getOptionValue("uri");
    String outputDirValue = commandLine.getOptionValue("output-dir");
    String principalVal = commandLine.getOptionValue("principal");
    String keyTabVal = commandLine.getOptionValue("keyTab");
    String[] systemPropertiesValue = commandLine.getOptionValues("system");
    String[] databasesValue = commandLine.getOptionValues("databases");
    String[] tablesValue = commandLine.getOptionValues("tables");
    String configPathValue = commandLine.getOptionValue("config");
    HiveMetaCarrier hiveMetaCarrier = new HiveMetaCarrier(uriValue,
                                                          outputDirValue,
                                                          principalVal,
                                                          keyTabVal,
                                                          systemPropertiesValue);

    MetaCarrierConfiguration config;
    if (configPathValue != null) {
      config = new MetaCarrierConfiguration(configPathValue);
      config.addDatabases(databasesValue);
      config.addTables(tablesValue);
    } else {
      config = new MetaCarrierConfiguration(databasesValue, tablesValue);
    }
    hiveMetaCarrier.carry(config);
  }
}
