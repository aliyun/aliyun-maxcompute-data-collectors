package com.aliyun.odps.datacarrier.metacarrier;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.datacarrier.commons.DirUtils;

public class MetaCarrierConfiguration {
  private static final String DB_TABLE_NAME_SEPARATOR = ".";

  private Set<String> databases = new HashSet<>();
  private Map<String, HashSet<String>> databaseToTables = new HashMap<>();

  public MetaCarrierConfiguration(String configPath) throws IOException {
    if (configPath == null || configPath.length() == 0) {
      throw new IllegalArgumentException("Config cannot be null or empty");
    }
    parse(configPath);
  }

  public MetaCarrierConfiguration(String[] databases, String[] tables) {
    if (databases != null) {
      this.databases.addAll(Arrays.asList(databases));
    }
    if (tables != null) {
      for (String table : tables) {
        parseLine(table);
      }
    }
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

    // TODO: support including a whole database & excluding a table
    int idx = line.indexOf(DB_TABLE_NAME_SEPARATOR);
    if (idx != -1 && idx != line.length() - 1) {
      String database = line.substring(0, idx);
      String table = line.substring(idx + 1);
      if (!databaseToTables.containsKey(database)) {
        databaseToTables.put(database, new HashSet<>());
      }
      databaseToTables.get(database).add(table);
    } else {
      throw new IllegalArgumentException(
          "Each line should be in the following format: <hive db>.<hive table>");
    }
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
    if (databaseToTables.containsKey(database)
        && databaseToTables.get(database).contains(table)) {
      return true;
    }

    return false;
  }

  public boolean shouldCarry(String database) {
    if (database == null) {
      throw new IllegalArgumentException("Argument \'database\' cannot be null");
    }

    if (databases.contains(database) || databaseToTables.containsKey(database)) {
      return true;
    }

    return false;
  }
}
