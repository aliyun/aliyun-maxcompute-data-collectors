package com.aliyun.odps.datacarrier.metacarrier;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
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
  private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(".*\\((.*)\\)");

  private Set<String> databases = new HashSet<>();
  // Database name -> table name -> list of partition spec
  private Map<String, Map<String, List<Map<String, String>>>> tables = new HashMap<>();

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

      Matcher m = TABLE_NAME_PATTERN.matcher(table);
      if (m.matches()) {
        partitionSpec = parsePartitionSpec(m.group(1));
        table = table.substring(0, table.length() - m.group(1).length() - 2);
      }

      if (!tables.containsKey(database)) {
        tables.put(database, new HashMap<>());
      }
      if (partitionSpec != null) {
        if (!tables.get(database).containsKey(table)) {
          tables.get(database).put(table, new LinkedList<>());
        }
        tables.get(database).get(table).add(partitionSpec);
      } else {
        tables.get(database).put(table, null);
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

  public void addDatabases(String[] databases) {
    if (databases != null) {
      this.databases.addAll(Arrays.asList(databases));
    }
  }

  public void addTables(String[] tables) {
    if (tables != null) {
      for (String table : tables) {
        parseLine(table);
      }
    }
  }

  public boolean shouldCarry(String database, String table) {
    if (database == null || table == null) {
      throw new IllegalArgumentException("Argument \'database\' or \'table\' cannot be null");
    }

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

    if (databases.contains(database) || tables.containsKey(database)) {
      return true;
    }

    return false;
  }

  public List<Map<String, String>> getPartitionsToCarry(String database, String table) {
    if (!database.contains(database) &&
        !(tables.containsKey(database) && tables.get(database).containsKey(table))) {
      throw new IllegalArgumentException("Should not carry this table: " + database + "." + table);
    }

    if (tables.containsKey(database) && tables.get(database).containsKey(table)) {
      return tables.get(database).get(table);
    } else {
      return null;
    }
  }
}
