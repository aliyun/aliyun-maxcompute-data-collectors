package com.aliyun.odps.datacarrier.metacarrier;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.datacarrier.commons.DirUtils;

public class MetaCarrierConfiguration {
  private static final String DB_TABLE_NAME_SEPARATOR = ".";
  private static final String NUM_OF_PARTITIONS = "numOfPartitions";
  //test_db.test_table(pc1=pv1,pc2=pv2){setting1=value1,setting2=value2}:odps_data_carrier_test.test_table
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(".*\\((.*)\\)");
  private static final Pattern TABLE_CONF_PATTERN = Pattern.compile(".*\\{(.*)}");

  private Set<String> databases = new HashSet<>();
  // Database name -> table name -> list of partition spec
  private Map<String, Map<String, MetaCarrierTableConfiguration>> tables = new HashMap<>();
  private int defaultNumOfPartitions = 0;

  public class MetaCarrierTableConfiguration {
    private int numOfPartitions = 0;
    private List<Map<String, String>> partitionSpec = new LinkedList<>();

    public List<Map<String, String>> getPartitionSpec() {
      return partitionSpec;
    }

    public int getNumOfPartitions() {
      return numOfPartitions;
    }

    public void setNumOfPartitions(int numOfPartitions) {
      this.numOfPartitions = numOfPartitions;
    }
  }


  public MetaCarrierConfiguration() {
  }

  public void load(String configPath) throws IOException {
    if (configPath == null || configPath.length() == 0) {
      throw new IllegalArgumentException("Config cannot be null or empty");
    }
    parse(configPath);
  }

  private void parse(String configPath) throws IOException {
    BufferedReader reader = DirUtils.getLineReader(Paths.get(configPath));

    String line;
    while((line = reader.readLine()) != null) {
      parseLine(line);
    }
  }

  private void parseLine(String line) {
    if (line == null || line.length() == 0) {
      return;
    }

    // Should be in pattern <db name>.<tbl name>[(pc1=pv1,pc2=pv2)]
    // TODO: support including a whole database & excluding a table
    int idx = line.indexOf(DB_TABLE_NAME_SEPARATOR);
    if (idx != -1 && idx != line.length() - 1) {
      String database = line.substring(0, idx);
      String table = line.substring(idx + 1);
      Map<String, String> partitionSpec = null;
      System.out.println("### table : " + table);
      Matcher m = TABLE_NAME_PATTERN.matcher(table);
      if (m.matches() && !StringUtils.isBlank(m.group(1))) {
        partitionSpec = parsePartitionSpec(m.group(1));
        table = table.substring(0, table.length() - m.group(1).length() - 2);
        System.out.println("### has partition : " + table + "(" + partitionSpec + ")");
      }

      Map<String, String> tableConf = new HashMap<>();
      Matcher confMatcher = TABLE_CONF_PATTERN.matcher(table);
      if (confMatcher.matches()) {
        parseTableConfSpec(confMatcher.group(1), tableConf);
        table = table.substring(0, table.length() - confMatcher.group(1).length() - 2);
        System.out.println("### has conf : " + table + tableConf.toString());
      }

      if (!tables.containsKey(database)) {
        tables.put(database.toLowerCase(), new HashMap<>());
      }

      if (!tables.get(database).containsKey(table)) {
        tables.get(database.toLowerCase()).put(table.toLowerCase(),
                                               new MetaCarrierTableConfiguration());
      }

      MetaCarrierTableConfiguration tableConfiguration = tables.get(database).get(table);

      if (partitionSpec != null) {
        tableConfiguration.getPartitionSpec().add(partitionSpec);
      }
      System.out.print("### defaultNumOfPartitions : " + defaultNumOfPartitions + "\n");
      if (defaultNumOfPartitions > 0) {
        tableConfiguration.setNumOfPartitions(defaultNumOfPartitions);
      }
      if (tableConf.containsKey(NUM_OF_PARTITIONS)) {
        int tableConfNumsOfPartitions = Integer.parseInt(tableConf.get(NUM_OF_PARTITIONS));
        System.out.print("### tableConfNumsOfPartitions : " + tableConfNumsOfPartitions + "\n");
        if (tableConfNumsOfPartitions > 0) {
          tableConfiguration.setNumOfPartitions(Integer.parseInt(tableConf.get(NUM_OF_PARTITIONS)));
        }
      }

    } else {
      throw new IllegalArgumentException(
          "Each line should be in the following format: <hive db>.<hive table>");
    }
  }

  private Map<String, String> parsePartitionSpec(String partitionSpec) {
    if (StringUtils.isBlank(partitionSpec)) {
      throw new IllegalArgumentException("Invalid partition spec" + partitionSpec);
    }

    Map<String, String> ps = new LinkedHashMap<>();
    String[] entries = partitionSpec.split(",");
    for (String entry : entries) {
      String[] kv = entry.trim().split("=");
      if (kv.length != 2) {
        throw new IllegalArgumentException("Invalid partition spec" + partitionSpec);
      }
      ps.put(kv[0].trim(), kv[1].trim());
    }

    return ps;
  }

  private void parseTableConfSpec(String tableConfSpec, Map<String, String> tableConf) {
    if (StringUtils.isBlank(tableConfSpec)) {
      return;
    }
    String[] confs = tableConfSpec.split(",");
    for(String conf : confs) {
      String[] kv = conf.trim().split("=");
      if (kv.length != 2) {
        continue;
      }
      tableConf.putIfAbsent(kv[0].trim(), kv[1].trim());
    }
  }

  public void addDatabases(String[] databases) {
    if (databases != null) {
      for (String database : databases) {
        this.databases.add(database.toLowerCase());
      }
    }
  }

  public void addTables(String[] tables) {
    if (tables != null) {
      for (String table : tables) {
        parseLine(table);
      }
    }
  }

  public void setDefaultNumOfPartitions(int defaultNumOfPartitions) {
    this.defaultNumOfPartitions = defaultNumOfPartitions;
  }

  public boolean shouldCarry(String database, String table) {
    if (database == null || table == null) {
      throw new IllegalArgumentException("Argument \'database\' or \'table\' cannot be null");
    }

    database = database.toLowerCase();
    table = table.toLowerCase();

    if (databases.contains(database)) {
      return true;
    }
    if (tables.containsKey(database) && tables.get(database).containsKey(table)) {
      return true;
    }

    return false;
  }

  public boolean shouldCarry(String database) {
    if (database == null) {
      throw new IllegalArgumentException("Argument \'database\' cannot be null");
    }

    database = database.toLowerCase();

    if (databases.contains(database) || tables.containsKey(database)) {
      return true;
    }

    return false;
  }

  public MetaCarrierTableConfiguration getPartitionsToCarry(String database, String table) {
    if (!database.contains(database) &&
        !(tables.containsKey(database) && tables.get(database).containsKey(table))) {
      throw new IllegalArgumentException("Should not carry this table: " + database + "." + table);
    }

    if (tables.containsKey(database) && tables.get(database).containsKey(table)) {
      return tables.get(database).get(table);
    }
    return null;
  }
}
