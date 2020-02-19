package com.aliyun.odps.datacarrier.commons;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

/**
 * Example:
 * {
 *   "type": "Local",
 *   "path": "/path/to/meta_carrier_output"
 * }
 */

public class LocalMetaSource implements MetaSource {
  private String path;
  // TODO: need to know where the metadata comes from
  public static LocalMetaSource fromJson(JsonObject jsonObject) {
    LocalMetaSource localMetaSource = new LocalMetaSource();

    localMetaSource.path = JsonUtils.getMemberAsString(jsonObject,
                                                       "path",
                                                       true);
    if (StringUtils.isEmpty(localMetaSource.path)) {
      throw new IllegalArgumentException("path cannot be empty");
    }

    return localMetaSource;
  }

  public String getPath() {
    return path;
  }
}
