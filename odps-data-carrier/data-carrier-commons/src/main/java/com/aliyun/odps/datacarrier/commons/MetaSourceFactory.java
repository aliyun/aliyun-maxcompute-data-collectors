package com.aliyun.odps.datacarrier.commons;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

public class MetaSourceFactory {
  public enum MetaSourceType {
    /**
     * Hive meta source
     */
    Hive,
    /**
     * Local file meta source
     */
    Local,
  }

  public static MetaSource fromJson(JsonObject jsonObject) {
    String type = JsonUtils.getMemberAsString(jsonObject,
                                              "type",
                                              true);
    if (StringUtils.isEmpty(type)) {
      throw new IllegalArgumentException("Meta source type cannot be empty");
    }

    if (MetaSourceType.Hive.name().equalsIgnoreCase(type)) {
      return HiveMetaSource.fromJson(jsonObject);
    } else if (MetaSourceType.Local.name().equalsIgnoreCase(type)) {
      return LocalMetaSource.fromJson(jsonObject);
    } else {
      throw new IllegalArgumentException("Unsupported data source: " + type);
    }
  }
}
