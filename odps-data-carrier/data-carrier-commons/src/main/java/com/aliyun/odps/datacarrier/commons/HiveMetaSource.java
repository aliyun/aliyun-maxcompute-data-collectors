package com.aliyun.odps.datacarrier.commons;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

/**
 * Example:
 * {
 *   "type": "Hive",
 *   "hms_thrift_addr": "thrift://127.0.0.1:9083"
 * }
 */
public class HiveMetaSource implements MetaSource {

  private String hmsThriftAddr;
  private String hmsKrbPrincipal;
  private String[] hmsKrbSystemProperties;

  public static HiveMetaSource fromJson(JsonObject jsonObject) {
    HiveMetaSource hiveMetaSource = new HiveMetaSource();

    hiveMetaSource.hmsThriftAddr = JsonUtils.getMemberAsString(jsonObject,
                                                               "hms_thrift_addr",
                                                               true);
    if (StringUtils.isEmpty(hiveMetaSource.hmsThriftAddr)) {
      throw new IllegalArgumentException("HMS thrift address cannot be empty");
    }

    hiveMetaSource.hmsKrbPrincipal = JsonUtils.getMemberAsString(jsonObject,
                                                                 "hms_krb_principal",
                                                                 false);
    if (StringUtils.isEmpty(hiveMetaSource.hmsKrbPrincipal)) {
      hiveMetaSource.hmsKrbPrincipal = null;
    }

    hiveMetaSource.hmsKrbSystemProperties = JsonUtils.getMemberAsStringArray(
        jsonObject, "hms_krb_system_properties", false);
    if (hiveMetaSource.hmsKrbSystemProperties != null
        && hiveMetaSource.hmsKrbSystemProperties.length == 0) {
      hiveMetaSource.hmsKrbSystemProperties = null;
    }

    return hiveMetaSource;
  }

  public String getHmsKrbPrincipal() {
    return hmsKrbPrincipal;
  }

  public String getHmsThriftAddr() {
    return hmsThriftAddr;
  }

  public String[] getHmsKrbSystemProperties() {
    return hmsKrbSystemProperties;
  }
}
