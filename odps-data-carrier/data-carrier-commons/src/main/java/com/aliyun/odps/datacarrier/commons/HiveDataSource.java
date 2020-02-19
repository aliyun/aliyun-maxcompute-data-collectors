package com.aliyun.odps.datacarrier.commons;

import com.google.gson.JsonObject;

/**
 * Example:
 * {
 *   "type": "Hive"
 * }
 */
public class HiveDataSource implements DataSource {
  public static HiveDataSource fromJson(JsonObject jsonObject) {
    return new HiveDataSource();
  }
}
