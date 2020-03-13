package com.aliyun.odps.datacarrier.taskscheduler;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ACCESS_ID;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ACCESS_KEY;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.END_POINT;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.META_CONFIG_FILE;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ODPS_CONFIG;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.PROJECT_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.TABLE_MAPPING;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.TUNNEL_ENDPOINT;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.Config;
import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.HiveConfiguration;
import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.OdpsConfiguration;
import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.TableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.TableGroup;
import com.aliyun.odps.utils.StringUtils;

public class MetaConfigurationUtils {

  private static final List<String> hiveJdbcExtraSettings = new ArrayList<String>() {
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

  public static MetaConfiguration readConfigFile(File configFile) throws IOException {
    if (!configFile.exists()) {
      throw new RuntimeException("Config file not exists (yet)");
    }
    FileInputStream inputStream = new FileInputStream(configFile);
    String content = new String(org.apache.commons.io.IOUtils.toByteArray(inputStream));
    return GsonUtils.getFullConfigGson().fromJson(content, MetaConfiguration.class);
  }

  public static MetaConfiguration generateSampleMetaConfiguration(String tableMappingFilePath,
                                                                  String odpsConfigFilePath)
      throws IOException {
    MetaConfiguration metaConfiguration = new MetaConfiguration("Jerry",
                                                                "TestMigrationJob",
                                                                DataSource.Hive);
    HiveConfiguration hiveConfiguration =
        new HiveConfiguration("jdbc:hive2://127.0.0.1:10000/default",
                              "Hive",
                              "",
                              "thrift://127.0.0.1:9083",
                              "",
                              "",
                              new String[]{}, hiveJdbcExtraSettings);
    metaConfiguration.setHiveConfiguration(hiveConfiguration);

    if (StringUtils.isNullOrEmpty(odpsConfigFilePath)) {
      OdpsConfiguration odpsConfiguration = new OdpsConfiguration("accessId",
                                                                  "accessKey",
                                                                  "endpoint",
                                                                  "projectName",
                                                                  "");
      metaConfiguration.setOdpsConfiguration(odpsConfiguration);
    } else {
      if (!odpsConfigFilePath.trim().startsWith("/")) {
        odpsConfigFilePath = Paths.get(odpsConfigFilePath).toAbsolutePath().toString();
      }
      InputStream is = new FileInputStream(odpsConfigFilePath);
      Properties properties = new Properties();
      properties.load(is);
      OdpsConfiguration odpsConfiguration = new OdpsConfiguration(
          properties.getProperty(ACCESS_ID),
          properties.getProperty(ACCESS_KEY),
          properties.getProperty(END_POINT),
          properties.getProperty(PROJECT_NAME),
          properties.containsKey(TUNNEL_ENDPOINT) ? properties.getProperty(TUNNEL_ENDPOINT) : "");
      metaConfiguration.setOdpsConfiguration(odpsConfiguration);
    }

    Config defaultTableConfig = new Config(null, null, 10, 3, "");

    List<TableGroup> tablesGroupList = new ArrayList<>();
    if (StringUtils.isNullOrEmpty(tableMappingFilePath)) {
      TableGroup tablesGroup = new TableGroup();
      List<TableConfig> tables = new ArrayList<>();
      TableConfig table = new TableConfig("SourceDataBase", "SourceTable", "DestProject", "DestTable", defaultTableConfig);
      tables.add(table);
      tablesGroup.setTables(tables);
      tablesGroup.setGroupConfig(defaultTableConfig);
      tablesGroupList.add(tablesGroup);
    } else {
      if (!tableMappingFilePath.trim().startsWith("/")) {
        tableMappingFilePath = Paths.get(tableMappingFilePath).toAbsolutePath().toString();
      }
      Map<String, Map<String, Map<String, String>>> tableMap = parseTableMapping(tableMappingFilePath);
      for (Map.Entry<String, Map<String, Map<String, String>>> sourceDataBaseEntry : tableMap.entrySet()) {
        String sourceDataBase = sourceDataBaseEntry.getKey();
        for (Map.Entry<String, Map<String, String>> destProjectEntry : sourceDataBaseEntry.getValue().entrySet()) {
          String destinationProject = destProjectEntry.getKey();
          TableGroup tablesGroup = new TableGroup();
          List<TableConfig> tables = new ArrayList<>();
          for (Map.Entry<String, String> tableNameEntry : destProjectEntry.getValue().entrySet()) {
            TableConfig table = new TableConfig(sourceDataBase,
                tableNameEntry.getKey(),
                destinationProject,
                tableNameEntry.getValue(),
                defaultTableConfig);
            tables.add(table);
          }
          tablesGroup.setTables(tables);
          tablesGroup.setGroupConfig(defaultTableConfig);
          tablesGroupList.add(tablesGroup);
        }
      }
    }
    metaConfiguration.setTableGroups(tablesGroupList);
    metaConfiguration.setGlobalTableConfig(defaultTableConfig);

    return metaConfiguration;
  }

  public static void generateConfigFile(File configFile, String tableMappingFilePath, String odpsConfigFilePath)
      throws Exception {
    Files.deleteIfExists(configFile.toPath());
    assert configFile.createNewFile();
    FileOutputStream outputStream = new FileOutputStream(configFile);
    outputStream.write(GsonUtils.getFullConfigGson().toJson(
        generateSampleMetaConfiguration(tableMappingFilePath, odpsConfigFilePath),
        MetaConfiguration.class).getBytes());
    outputStream.close();
  }

  private static Map<String, Map<String, Map<String, String>>> parseTableMapping(String tableMappingFilePathStr) {
    Path tableMappingFilePath = Paths.get(tableMappingFilePathStr);
    // source.database -> (dest.project -> (source.database.table -> dest.database.table))
    Map<String, Map<String, Map<String, String>>> tableMap = new HashMap<>();
    try {
      List<String> tableMappings = Files.readAllLines(tableMappingFilePath);
      for (String tableMappingStr : tableMappings) {
        String[] mappings = tableMappingStr.split(":");
        if (mappings.length != 2) {
          continue;
        }
        String sourceDataBase = mappings[0].split("[.]")[0];
        String sourceTable = mappings[0].split("[.]")[1];
        String destProject = mappings[1].split("[.]")[0];
        String destTable = mappings[1].split("[.]")[1];

        tableMap.putIfAbsent(sourceDataBase, new HashMap<>());
        Map<String, Map<String, String>> destProjectMap = tableMap.get(sourceDataBase);
        destProjectMap.putIfAbsent(destProject, new HashMap<>());
        Map<String, String> sourceTableMap = destProjectMap.get(destProject);
        sourceTableMap.putIfAbsent(sourceTable, destTable);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return tableMap;
  }

  public static void main(String[] args) throws Exception {
    Option input = Option
        .builder()
        .longOpt(TABLE_MAPPING)
        .argName(TABLE_MAPPING)
        .hasArg()
        .desc("generate config.json for tables specified in table mapping file.")
        .build();
    Option odpsConfig = Option
        .builder()
        .longOpt(ODPS_CONFIG)
        .argName(ODPS_CONFIG)
        .hasArg()
        .desc("set OdpsConfiguration in config.json from odps_config.ini.")
        .build();
    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();

    Options options = new Options()
        .addOption(input)
        .addOption(odpsConfig)
        .addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP)) {
      logHelp(options);
      System.exit(0);
    }

    if (!cmd.hasOption(TABLE_MAPPING) || !cmd.hasOption(ODPS_CONFIG)) {
      throw new IllegalArgumentException("Required arguments: 'table-mapping' and 'odps-config'");
    }

    File outputFile = new File(META_CONFIG_FILE);
    generateConfigFile(outputFile,
                       cmd.getOptionValue(TABLE_MAPPING),
                       cmd.getOptionValue(ODPS_CONFIG));
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "generate-config --table-mapping <path to table mapping file> --odps-config <path to odps config>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
