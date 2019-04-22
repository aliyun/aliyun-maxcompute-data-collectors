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

//  private static final String
//  private static final String
//  private static final String
//  private static final String
//  private static final String
//  private static final String
//  private static final String
//  private static final String

  public static String toOdpsType(String hiveType, String odpsVersion) {
    hiveType = hiveType.toUpperCase();
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
      return "ARRAY" + "<" + toOdpsType(matcher.group(1), odpsVersion) + ">";
//    } else if (hiveType.matches(MAP)) {
//      Pattern pattern = Pattern.compile(MAP);
//      Matcher matcher = pattern.matcher(hiveType);
//      matcher.matches();
//      // TODO: cannot simply split with comma
//      String[] types = matcher.group(1).split(",");
//      return "MAP<" + toOdpsType(types[0].trim(), odpsVersion) + "," +
//          toOdpsType(types[1].trim(), odpsVersion) + ">";
//    } else if (hiveType.matches(STRUCT)) {
//      Pattern pattern = Pattern.compile(STRUCT);
//      Matcher matcher = pattern.matcher(hiveType);
//      matcher.matches();
//      List<String> fields = new ArrayList<>();
//
//
//      List<String> odpsFields = new ArrayList<>();
//      for (String field1 : fields) {
//        String field = field1;
//        // Remove comments, not supported
//        int commentIdx = field.toUpperCase().indexOf("COMMENT");
//        if (commentIdx != -1) {
//          field = field.substring(0, commentIdx);
//        }
//
//        // Convert to odps type
//        String[] fieldSplit = field.split(":");
//        String fieldName = fieldSplit[0].trim();
//        String fieldType = fieldSplit[1].trim();
//
//        odpsFields.add(fieldName + ":" + toOdpsType(fieldType, odpsVersion));
//      }
//      return "STRUCT<" + String.join(", ", odpsFields) + ">";
    } else {
      throw new IllegalArgumentException("Invalid HIVE type: " + hiveType);
    }
  }

//  private static List<String> splitMapFields(String mapFields) {
//    int angleBracketsCounter = 0;
//    int startIdx = 0;
//    List<String> fields;
//
//    for (int i = 0; i < mapFields.length(); i++) {
//      if (mapFields.charAt(i) == '<') {
//
//      }
//    }
//
//    return null;
//  }

  public static void main(String[] args) {
    String odpsType = HiveTypeTransformer.toOdpsType("array<array<struct<x:int comment \"fuck\", y:map<string, date> comment \"this world\">>>", "2.0");
    System.out.println(odpsType);
  }
}
