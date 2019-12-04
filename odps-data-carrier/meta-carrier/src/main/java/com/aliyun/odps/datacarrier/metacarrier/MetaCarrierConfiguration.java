package com.aliyun.odps.datacarrier.metacarrier;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.odps.datacarrier.commons.DirUtils;

public class MetaCarrierConfiguration {
  private static final String DB_TABLE_NAME_SEPARATOR = ".";

  private Set<String> databases = new HashSet<>();
  private Set<String> tables = new HashSet<>();

  public MetaCarrierConfiguration(String configPath) throws IOException {
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

    // TODO: support including a whole database & excluding a table
    tables.add(line);
  }

  public MetaCarrierConfiguration(String[] databases, String[] tables) {
    if (databases != null) {
      for (String database : databases) {
        System.out.println(database);
      }
      this.databases.addAll(Arrays.asList(databases));
    }
    if (tables != null) {
      for (String table : tables) {
        System.out.println(table);
      }
      this.tables = new HashSet<>(Arrays.asList(tables));
    }
  }

  public void addDatabases(String[] databases) {
    if (databases != null) {
      this.databases.addAll(Arrays.asList(databases));
    }
  }

  public void addTables(String[] tables) {
    if (tables != null) {
      this.tables.addAll(Arrays.asList(tables));
    }
  }

  public boolean shouldCarry(String database, String table) {
    if (database == null || table == null) {
      throw new IllegalArgumentException("Argument \'database\' or \'table\' cannot be null");
    }

    if (databases.contains(database)) {
      return true;
    }
    if (tables.contains(database + DB_TABLE_NAME_SEPARATOR + table)) {
      return true;
    }

    return false;
  }
}
