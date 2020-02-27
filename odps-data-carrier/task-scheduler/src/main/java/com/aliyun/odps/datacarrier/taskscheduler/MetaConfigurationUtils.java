package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MetaConfiguration.*;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.*;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;

public class MetaConfigurationUtils {

  public static MetaConfiguration readConfigFile(File configFile) throws IOException {
    if (!configFile.exists()) {
      throw new RuntimeException("Config file not exists (yet)");
    }
    FileInputStream inputStream = new FileInputStream(configFile);
    String content = new String(org.apache.commons.io.IOUtils.toByteArray(inputStream));
    return GsonUtils.getFullConfigGson().fromJson(content, MetaConfiguration.class);
  }

  public static MetaConfiguration generateSampleMetaConfiguration(String tableMappingFilePath) {
    MetaConfiguration metaConfiguration = new MetaConfiguration("Jerry", "TestMigrationJob", DataSource.Hive);
    HiveConfiguration hiveConfiguration = new HiveConfiguration("jdbc:hive2://127.0.0.1:10000/default", "Hive", "",
        "thrift://127.0.0.1:9083", "", "", new String[]{""});
    metaConfiguration.setHiveConfiguration(hiveConfiguration);

    OdpsConfiguration odpsConfiguration = new OdpsConfiguration("", "", "", "", "");
    metaConfiguration.setOdpsConfiguration(odpsConfiguration);
    Config defaultTableConfig = new Config(null, null, 10, 5, "");

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

  public static File getDefaultConfigFile() {
    // TODO: use a fixed parent directory
    String currentDir = System.getProperty("user.dir");
    return new File(currentDir + "/" + ODPS_DATA_CARRIER, META_CONFIG_FILE);
  }

  public static void generateConfigFile(File configFile, String tableMappingFilePath) throws Exception {
    Files.deleteIfExists(configFile.toPath());
    configFile.createNewFile();
    FileOutputStream outputStream = new FileOutputStream(configFile);
    outputStream.write(GsonUtils.getFullConfigGson().toJson(generateSampleMetaConfiguration(tableMappingFilePath),
        MetaConfiguration.class).getBytes());
    outputStream.close();
  }

  private static Map<String, Map<String, Map<String, String>>> parseTableMapping(String tableMappingFilePathStr) {
    Path tableMappingFilePath = Paths.get(System.getProperty("user.dir"), tableMappingFilePathStr);
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
        .builder("input")
        .longOpt(TABLE_MAPPING)
        .argName(TABLE_MAPPING)
        .hasArg()
        .desc("generate config.json for tables specified in table mapping file.")
        .build();
    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();
    Options options = new Options()
        .addOption(input)
        .addOption(help);
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (!cmd.hasOption(HELP)) {
      File configFile = getDefaultConfigFile();
      if (cmd.hasOption(TABLE_MAPPING)) {
        configFile = new File(System.getProperty("user.dir"), META_CONFIG_FILE);
      }
      generateConfigFile(configFile, cmd.getOptionValue(TABLE_MAPPING));
    } else {
      logHelp(options);
    }
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "generate-config -input <table mapping file path>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
