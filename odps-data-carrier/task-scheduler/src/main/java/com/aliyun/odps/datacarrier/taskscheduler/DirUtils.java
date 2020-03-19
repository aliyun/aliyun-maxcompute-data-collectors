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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

// TODO: check null
public class DirUtils {

  /**
   * Write a file. Its parent directories will be created if they doesn't exist.
   *
   * @param filePath path to the file
   * @param content content
   * @throws IllegalArgumentException if any argument is null
   * @throws IOException if any IOException happened
   */
  public static void writeFile(Path filePath, String content) throws IOException {
    writeFile(filePath, content, false);
  }

  public static void appendToFile(Path filePath, String content) throws IOException {
    writeFile(filePath, content, true);
  }

  public static void writeFile(Path filePath, String content, boolean append) throws IOException {
    File file = filePath.toAbsolutePath().toFile();
    File parent = file.getParentFile();
    if (!file.getParentFile().exists()) {
      if(!file.getParentFile().mkdirs()) {
        throw new IOException(parent.getAbsolutePath() + " does not exist and cannot be created.");
      }
    }

    FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8, append);
  }

  public static void writeCsvFile(Path filePath, List<List<String>> rows) throws IOException {
    writeCsvFile(filePath, rows, false);
  }

  public static void appendToCsvFile(Path filePath, List<List<String>> rows) throws IOException {
    writeCsvFile(filePath, rows, true);
  }

  public static void writeCsvFile(Path filePath, List<List<String>> rows, boolean append)
      throws IOException {
    File file = filePath.toAbsolutePath().toFile();
    File parent = file.getParentFile();
    if (!file.getParentFile().exists()) {
      if(!file.getParentFile().mkdirs()) {
        throw new IOException(parent.getAbsolutePath() + " does not exist and cannot be created.");
      }
    }

    try (FileOutputStream fos = new FileOutputStream(filePath.toFile(), append)) {
      CsvWriter csvWriter = new CsvWriter(fos, ',', StandardCharsets.UTF_8);
      for (List<String> partitionValues : rows) {
        csvWriter.writeRecord(partitionValues.toArray(new String[0]));
      }
      csvWriter.close();
    }
  }

  public static String readFile(Path filePath) throws IOException {
    return FileUtils.readFileToString(filePath.toFile(), StandardCharsets.UTF_8);
  }

  public static List<List<String>> readCsvFile(Path filePath) throws IOException {
    try (FileInputStream fis = new FileInputStream(filePath.toFile())) {
      List<List<String>> rows = new LinkedList<>();
      CsvReader csvReader = new CsvReader(fis, ',', StandardCharsets.UTF_8);
      while (csvReader.readRecord()) {
        rows.add(Arrays.asList(csvReader.getValues()));
      }
      return rows;
    }
  }

  public static List<String> readLines(Path filePath) throws IOException {
    return FileUtils.readLines(filePath.toFile(), StandardCharsets.UTF_8);
  }

  public static String[] listDirs(Path dir) {
    String[] items = dir.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return new File(dir, name).isDirectory();
      }
    });
    if (items == null) {
      throw new IllegalArgumentException(dir.toString() + " is not a valid directory.");
    }
    return items;
  }

  public static boolean removeDir(Path path) {
    if (path == null) {
      throw new IllegalArgumentException("'dir' cannot be null");
    }

    File[] files = path.toFile().listFiles();
    boolean ret = true;

    // Remove files and subdirs
    for (File f : files) {
      if (f.isDirectory()) {
        ret = ret & removeDir(f.toPath());
      } else {
        ret = ret & remove(f.toPath());
      }
    }
    // Remove itself
    ret = ret & path.toFile().delete();

    return ret;
  }

  public static boolean remove(Path path) {
    if (path == null) {
      throw new IllegalArgumentException("'path' cannot be null");
    }

    if (!path.toFile().isFile()) {
      throw new IllegalArgumentException("'path' cannot be a directory");
    }

    return path.toFile().delete();
  }

  public static String[] listFiles(Path dir) {
    String[] items = dir.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return new File(dir, name).isFile();
      }
    });
    if (items == null) {
      throw new IllegalArgumentException(dir.toString() + " is not a valid directory.");
    }
    return items;
  }

  public static String[] listFiles(Path dir, String prefix) {
    String[] items = dir.toFile().list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return new File(dir, name).isFile() && name.startsWith(prefix);
      }
    });
    if (items == null) {
      throw new IllegalArgumentException(dir.toString() + " is not a valid directory.");
    }
    return items;
  }
}
