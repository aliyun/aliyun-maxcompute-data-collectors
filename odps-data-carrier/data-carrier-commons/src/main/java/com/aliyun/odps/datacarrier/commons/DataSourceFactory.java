package com.aliyun.odps.datacarrier.commons;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

public class DataSourceFactory {
  public enum DataSourceType {
    /**
     * Hive data source
     */
    Hive,
    /**
     * OSS data source
     */
    OSS,
  }

  public static DataSource fromJson(JsonObject jsonObject) {
    String type = JsonUtils.getMemberAsString(jsonObject,
                                              "type",
                                              true);
    if (StringUtils.isEmpty(type)) {
      throw new IllegalArgumentException("Data source type cannot be empty");
    }

    if (DataSourceType.Hive.name().equalsIgnoreCase(type)) {
      return HiveDataSource.fromJson(jsonObject);
    } else if (DataSourceType.OSS.name().equalsIgnoreCase(type)) {
      return OSSDataSource.fromJson(jsonObject);
    } else {
      throw new IllegalArgumentException("Unsupported data source: " + type);
    }
  }
}
