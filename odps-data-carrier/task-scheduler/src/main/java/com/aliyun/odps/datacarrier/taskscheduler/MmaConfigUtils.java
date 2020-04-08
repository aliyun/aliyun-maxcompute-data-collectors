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

package com.aliyun.odps.datacarrier.taskscheduler;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ACCESS_ID;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ACCESS_KEY;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.END_POINT;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.PROJECT_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.TUNNEL_ENDPOINT;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.AdditionalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.HiveConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.OdpsConfig;
import com.aliyun.odps.utils.StringUtils;

public class MmaConfigUtils {

  public static final List<String> HIVE_JDBC_EXTRA_SETTINGS = new ArrayList<String>() {
    {
      add("hive.fetch.task.conversion=none");
      add("hive.execution.engine=mr");
      add("mapreduce.job.name=data-carrier");
      add("mapreduce.max.split.size=512000000");
      add("mapreduce.task.timeout=3600000");
      add("mapreduce.map.maxattempts=0");
      add("mapred.map.tasks.speculative.execution=false");
    }
  };

  public static final AdditionalTableConfig DEFAULT_ADDITIONAL_TABLE_CONFIG =
      new AdditionalTableConfig(
          null,
          null,
          100,
          3);

  public static final HiveConfig SAMPLE_HIVE_CONFIG =
      new HiveConfig("jdbc:hive2://127.0.0.1:10000/default",
                     "Hive",
                     "",
                     "thrift://127.0.0.1:9083",
                     null,
                     null,
                     null,
                     HIVE_JDBC_EXTRA_SETTINGS);

  public static final OdpsConfig SAMPLE_ODPS_CONFIG =
      new OdpsConfig("access ID",
                     "access key",
                     "endpoint",
                     "project name",
                     null);

  public static HiveConfig parseHiveConfig(Path hiveConfigPath) throws IOException {
    if (hiveConfigPath == null || !hiveConfigPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    Properties properties = new Properties();
    properties.load(new FileReader(hiveConfigPath.toFile()));
    return new HiveConfig(properties.getProperty("jdbc_connection_url"),
                          properties.getProperty("user"),
                          properties.getProperty("password"),
                          properties.getProperty("hms_thrift_addr"),
                          null,
                          null,
                          null,
                          HIVE_JDBC_EXTRA_SETTINGS);
  }

  public static OdpsConfig parseOdpsConfig(Path odpsConfigPath) throws IOException {
    if (odpsConfigPath == null || !odpsConfigPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    Properties properties = new Properties();
    properties.load(new FileReader(odpsConfigPath.toFile()));

    String tunnelEndpoint = properties.getProperty(TUNNEL_ENDPOINT);
    if (StringUtils.isNullOrEmpty(tunnelEndpoint)) {
      tunnelEndpoint = null;
    }

    return new OdpsConfig(properties.getProperty(ACCESS_ID),
                          properties.getProperty(ACCESS_KEY),
                          properties.getProperty(END_POINT),
                          properties.getProperty(PROJECT_NAME),
                          tunnelEndpoint);
  }

  public static List<MmaConfig.TableMigrationConfig> parseTableMapping(Path tableMappingPath)
      throws IOException {
    if (tableMappingPath == null || !tableMappingPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    List<String> lines = DirUtils.readLines(tableMappingPath);
    List<MmaConfig.TableMigrationConfig> tableMigrationConfigs = new LinkedList<>();
    // Used to remove duplications
    Map<String, String> sourceToDest = new HashMap<>();

    for (String line : lines) {
      line = line.trim();

      // Skip empty line and comment
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }

      String[] sourceAndDest = line.split(":");
      if (sourceAndDest.length != 2) {
        System.err.println("[WARN] Invalid line:" + line);
        continue;
      }

      String[] source = sourceAndDest[0].split("\\.");
      if (source.length != 2) {
        System.err.println("[WARN] Invalid source:" + line);
        continue;
      }

      String[] dest = sourceAndDest[1].split("\\.");
      if (dest.length != 2) {
        System.err.println("[WARN] Invalid destination:" + line);
        continue;
      }

      if (sourceToDest.containsKey(sourceAndDest[0])) {
        System.err.println("[WARN] Duplicated line:" + line);
        continue;
      }
      sourceToDest.put(sourceAndDest[0], sourceAndDest[1]);

      String sourceDataBase = source[0];
      String sourceTable = source[1];
      String destProject = dest[0];
      String destTable = dest[1];

      MmaConfig.TableMigrationConfig tableConfig =
          new MmaConfig.TableMigrationConfig(sourceDataBase,
                                             sourceTable,
                                             destProject,
                                             destTable,
                                             DEFAULT_ADDITIONAL_TABLE_CONFIG);

      tableMigrationConfigs.add(tableConfig);
    }

    return tableMigrationConfigs;
  }

  public static void generateMmaClientConfig(Path hiveConfigPath) throws IOException {
    String json = new MmaClientConfig(DataSource.Hive,
                                      null,
                                      parseHiveConfig(hiveConfigPath),
                                      null).toJson();
    DirUtils.writeFile(Paths.get("mma_client_config.json"), json);
  }

  public static void generateMmaServerConfig(Path hiveConfigPath,
                                             Path odpsConfigPath) throws IOException {
    String json = new MmaServerConfig(DataSource.Hive,
                                   null,
                                   parseHiveConfig(hiveConfigPath),
                                   parseOdpsConfig(odpsConfigPath)).toJson();
    DirUtils.writeFile(Paths.get("mma_server_config.json"), json);
  }

  public static void generateMmaMigrationConfig(Path tableMappingPath) throws IOException {
    String json =  new MmaMigrationConfig("Jerry",
                                          parseTableMapping(tableMappingPath),
                                          DEFAULT_ADDITIONAL_TABLE_CONFIG).toJson();
    DirUtils.writeFile(Paths.get("mma_migration_config.json"), json);
  }

  public static void generateSampleConfigs() throws IOException {
    MmaClientConfig mmaClientConfig = new MmaClientConfig(DataSource.Hive,
                                                          null,
                                                          SAMPLE_HIVE_CONFIG,
                                                          null);
    MmaServerConfig mmaServerConfig = new MmaServerConfig(DataSource.Hive,
                                                          null,
                                                          SAMPLE_HIVE_CONFIG,
                                                          SAMPLE_ODPS_CONFIG);

    MmaConfig.TableMigrationConfig tableMigrationConfig =
        new MmaConfig.TableMigrationConfig("source DB",
                                           "source table",
                                           "dest project",
                                           "dest table",
                                           DEFAULT_ADDITIONAL_TABLE_CONFIG);

    MmaMigrationConfig mmaMigrationConfig =
        new MmaMigrationConfig("Jerry",
                               Collections.singletonList(tableMigrationConfig),
                               DEFAULT_ADDITIONAL_TABLE_CONFIG);

    DirUtils.writeFile(Paths.get("mma_server_config.json"), mmaServerConfig.toJson());
    DirUtils.writeFile(Paths.get("mma_client_config.json"), mmaClientConfig.toJson());
    DirUtils.writeFile(Paths.get("mma_migration_config.json"), mmaMigrationConfig.toJson());
  }

  public static void main(String[] args) throws Exception {
    Option helpOption = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();

    Option sampleOption = Option
        .builder("sample")
        .longOpt("sample")
        .hasArg(false)
        .desc("generate sample server, client and migration configs")
        .build();

    Option clientOption = Option
        .builder("c")
        .longOpt("to_client_config")
        .hasArg(false)
        .desc("convert hive_config.ini specified by '--hive_config' to mma client config")
        .build();

    Option serverOption = Option
        .builder("s")
        .longOpt("to_server_config")
        .hasArg(false)
        .desc("convert hive_config.ini specified by '--hive_config' and "
              + "odps_config.ini specified by '--odps_config' to mma server config")
        .build();

    Option migrationOption = Option
        .builder("m")
        .longOpt("to_migration_config")
        .hasArg(false)
        .desc("convert table mapping specified by '--table_mapping' to mma migration config")
        .build();

    Option hiveConfigOption = Option
        .builder("hive_config")
        .longOpt("hive_config")
        .hasArg()
        .argName("Path to hive_config.ini")
        .desc("Path to hive_config.ini")
        .build();

    Option odpsConfigOption = Option
        .builder("odps_config")
        .longOpt("odps_config")
        .hasArg()
        .argName("Path to odps_config.ini")
        .desc("Path to odps_config.ini")
        .build();

    Option tableMappingOption = Option
        .builder("table_mapping")
        .longOpt("table_mapping")
        .hasArg()
        .argName("Path to table_mapping file")
        .desc("Path to table_mapping file")
        .build();

    Options options = new Options()
        .addOption(helpOption)
        .addOption(sampleOption)
        .addOption(clientOption)
        .addOption(serverOption)
        .addOption(migrationOption)
        .addOption(hiveConfigOption)
        .addOption(odpsConfigOption)
        .addOption(tableMappingOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP)) {
      logHelp(options);
      System.exit(0);
    }

    if (cmd.hasOption("sample")) {
      generateSampleConfigs();
      System.exit(0);
    }

    if (cmd.hasOption("to_client_config")) {
      if (!cmd.hasOption("hive_config")) {
        throw new IllegalArgumentException("Requires '--hive_config'");
      }

      generateMmaClientConfig(Paths.get(cmd.getOptionValue("hive_config")));
    }

    if (cmd.hasOption("to_server_config")) {
      if (!cmd.hasOption("hive_config") || !cmd.hasOption("odps_config")) {
        throw new IllegalArgumentException("Requires '--hive_config' and '--odps_config'");
      }

      generateMmaServerConfig(Paths.get(cmd.getOptionValue("hive_config")),
                              Paths.get(cmd.getOptionValue("odps_config")));
    }

    if (cmd.hasOption("to_migration_config")) {
      if (!cmd.hasOption("table_mapping")) {
        throw new IllegalArgumentException("Requires '--table_mapping'");
      }

      generateMmaMigrationConfig(Paths.get(cmd.getOptionValue("table_mapping")));
    }
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "generate-config --table-mapping <path to table mapping file> --odps-config <path to odps config>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
