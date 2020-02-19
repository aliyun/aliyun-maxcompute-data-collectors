package com.aliyun.odps.datacarrier.commons;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

/**
 * Example:
 * {
 *   "type": "OSS"
 *   "oss_endpoint": "oss-cn-hangzhou.aliyuncs.com",
 *   "oss_bucket": "mma-test"
 * }
 */
public class OSSDataSource implements DataSource {
  private String ossEndpoint;
  private String ossBucket;

  public static OSSDataSource fromJson(JsonObject jsonObject) {
    OSSDataSource ossDataSource = new OSSDataSource();


    ossDataSource.ossEndpoint = JsonUtils.getMemberAsString(jsonObject,
                                                            "oss_endpoint",
                                                            true);
    if (StringUtils.isEmpty(ossDataSource.ossEndpoint)) {
      throw new IllegalArgumentException("OSS endpoint cannot be empty");
    }

    ossDataSource.ossBucket = JsonUtils.getMemberAsString(jsonObject,
                                                          "oss_bucket",
                                                          true);
    if (StringUtils.isEmpty(ossDataSource.ossBucket)) {
      throw new IllegalArgumentException("OSS bucket cannot be empty");
    }

    return ossDataSource;
  }

  public String getOssBucket() {
    return ossBucket;
  }

  public String getOssEndpoint() {
    return ossEndpoint;
  }
}
