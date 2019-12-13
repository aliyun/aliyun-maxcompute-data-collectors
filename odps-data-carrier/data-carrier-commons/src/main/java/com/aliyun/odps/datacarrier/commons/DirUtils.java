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

package com.aliyun.odps.datacarrier.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;

public class DirUtils {
  /**
   * Write to a file specified by filePath, create its parent directories if they doesn't exist.
   */
  public static void writeFile(Path filePath, String content) throws IOException {
    writeFile(filePath, content, false);
  }

  public static void appendToFile(Path filePath, String content) throws IOException {
    writeFile(filePath, content, true);
  }

  public static void writeFile(Path filePath, String content, boolean append)
      throws IOException {
    File file = filePath.toFile();
    File parent = file.getParentFile();
    if (!file.getParentFile().exists()) {
      if(!file.getParentFile().mkdirs()) {
        throw new IOException(parent.getAbsolutePath() + " does not exist and cannot be created.");
      }
    }

    FileUtils.writeStringToFile(file, content, Constants.DEFAULT_CHARSET, append);
  }

  public static String readFile(Path filePath) throws IOException {
    return FileUtils.readFileToString(filePath.toFile(), Constants.DEFAULT_CHARSET);
  }

  public static BufferedReader getLineReader(Path filePath) throws IOException {
    return new BufferedReader(new FileReader(filePath.toFile()));
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
