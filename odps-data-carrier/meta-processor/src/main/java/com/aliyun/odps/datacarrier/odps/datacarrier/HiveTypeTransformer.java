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

package com.aliyun.odps.datacarrier.odps.datacarrier;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class HiveTypeTransformer {
  //TODO: support odps1.0
  // TODO: support hive.compatible
  /**
   * Numeric types
   */
  private static final String TINYINT = "TINYINT";
  private static final String SMALLINT = "SMALLINT";
  private static final String INT = "INT";
  private static final String BIGINT = "BIGINT";
  private static final String FLOAT = "FLOAT";
  private static final String DOUBLE = "DOUBLE";
  private static final String DECIMAL = "DECIMAL(\\([\\d]+,\\s*[\\d]+\\))?";

  /**
   * String types
   */
  private static final String STRING = "STRING";
  private static final String VARCHAR = "VARCHAR(\\([\\d]+\\))";
  private static final String CHAR = "CHAR(\\([\\d]+\\))";

  /**
   * Date and time types
   */
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String DATE = "DATE";

  /**
   * Misc types
   */
  private static final String BOOLEAN = "BOOLEAN";
  private static final String BINARY = "BINARY";

  /**
   * Complex types
   */
  private static final String ARRAY = "ARRAY<(.+)>";
  private static final String MAP = "MAP<(.+)>";
  private static final String STRUCT = "STRUCT<(.+)>";

  public static String toOdpsType(String hiveType, String odpsVersion) {
    hiveType = hiveType.toUpperCase().trim();
    if (hiveType.matches(TINYINT)) {
      return "TINYINT";
    } else if (hiveType.matches(SMALLINT)) {
      return "SMALLINT";
    } else if (hiveType.matches(INT)) {
      return "INT";
    } else if (hiveType.matches(BIGINT)) {
      return "BIGINT";
    } else if (hiveType.matches(FLOAT)) {
      return "FLOAT";
    } else if (hiveType.matches(DOUBLE)) {
      return "DOUBLE";
    } else if (hiveType.matches(DECIMAL)) {
      return "DECIMAL";
    } else if (hiveType.matches(TIMESTAMP)) {
      return "TIMESTAMP";
    } else if (hiveType.matches(DATE)) {
      // If odps version is 2.0 and hive.compatible is true, return DATE
      return "DATETIME";
    } else if (hiveType.matches(STRING)) {
      return "STRING";
    } else if (hiveType.matches(VARCHAR)) {
      Pattern pattern = Pattern.compile(VARCHAR);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      return "VARCHAR" + matcher.group(1);
    } else if (hiveType.matches(CHAR)) {
      Pattern pattern = Pattern.compile(CHAR);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      return "CHAR" + matcher.group(1);
    } else if (hiveType.matches(BOOLEAN)) {
      return "BOOLEAN";
    } else if (hiveType.matches(BINARY)) {
      return "BINARY";
    } else if (hiveType.matches(ARRAY)) {
      Pattern pattern = Pattern.compile(ARRAY);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      return "ARRAY" + "<" + toOdpsType(matcher.group(1).trim(), odpsVersion) + ">";
    } else if (hiveType.matches(MAP)) {

      Pattern pattern = Pattern.compile(MAP);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      // The type of key in a map must be a primitive type, so there is no comma in its type
      // definition. So we can split the type tuple of key and value by the first comma.
      String typeTuple = matcher.group(1);
      int firstCommaIdx = typeTuple.indexOf(',');
      String keyType = typeTuple.substring(0, firstCommaIdx).trim();
      String valueType = typeTuple.substring(firstCommaIdx + 1).trim();
      return "MAP<" + toOdpsType(keyType, odpsVersion) + "," +
          toOdpsType(valueType, odpsVersion) + ">";
    } else if (hiveType.matches(STRUCT)) {
      Pattern pattern = Pattern.compile(STRUCT);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      // Since the type definition of a struct can be very complex and may contain any possible
      // character in a type definition, we have to split the type list properly so that we can
      // handle them recursively later.
      List<String> fieldDefinitions = splitIntoSubTypes(matcher.group(1));

      List<String> odpsFieldDefinitions = new ArrayList<>();
      for (String fieldDefinition : fieldDefinitions) {
        // Remove comments, not supported
        int commentIdx = fieldDefinition.toUpperCase().indexOf("COMMENT");
        if (commentIdx != -1) {
          fieldDefinition = fieldDefinition.substring(0, commentIdx);
        }

        // The type of a struct field can be another struct, which may contain colons. So we have
        // to split the field definition by the first colon.
        int firstColonIdx = fieldDefinition.indexOf(':');
        String fieldName = fieldDefinition.substring(0, firstColonIdx).trim();
        String fieldType = fieldDefinition.substring(firstColonIdx + 1).trim();
        odpsFieldDefinitions.add(fieldName + ":" + toOdpsType(fieldType, odpsVersion));
      }
      return "STRUCT<" + String.join(",", odpsFieldDefinitions) + ">";
    } else {
      throw new IllegalArgumentException("Invalid HIVE type: " + hiveType);
    }
  }

  private static List<String> splitIntoSubTypes(String typeDefinition) {
    int angleBracketsCounter = 0;
    boolean split = true;
    int startIdx = 0;
    List<String> subTypes = new ArrayList<>();

    for (int i = 0; i < typeDefinition.length(); i++) {
      char c = typeDefinition.charAt(i);
      if (c == '<') {
        split = false;
        angleBracketsCounter += 1;
      } else if (c == '>') {
        angleBracketsCounter -= 1;
        if (angleBracketsCounter == 0) {
          split = true;
        }
      } else if (c == ',') {
        if (split) {
          subTypes.add(typeDefinition.substring(startIdx, i).trim());
          startIdx = i + 1;
        }
      }
    }
    subTypes.add(typeDefinition.substring(startIdx).trim());

    return subTypes;
  }
}
