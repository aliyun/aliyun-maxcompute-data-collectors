package com.aliyun.odps.datacarrier.commons;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class MMAConfiguration {

  private String migrationId;
  private DataSource dataSource;
  private MetaSource metaSource;
  private OdpsConfiguration odpsConfig;
  private List<RuleFactory.Rule> rules;

  private MMAConfiguration() {}

  public static MMAConfiguration fromJson(JsonObject jsonObject) {
    MMAConfiguration mmaConfiguration = new MMAConfiguration();

    mmaConfiguration.migrationId = JsonUtils.getMemberAsString(jsonObject,
                                                               "migration_id",
                                                               false);
    if (StringUtils.isEmpty(mmaConfiguration.migrationId)) {
      mmaConfiguration.migrationId = UUID.randomUUID().toString();
    }

    // Parse source config
    if (!jsonObject.has("source_config")) {
      throw new IllegalArgumentException("source_config cannot be null");
    } else {
      JsonObject sourceConfig = jsonObject.get("source_config").getAsJsonObject();
      if (!sourceConfig.has("data_source")) {
        throw new IllegalArgumentException("data_source cannot be null");
      } else {
        mmaConfiguration.dataSource = DataSourceFactory.fromJson(
            sourceConfig.get("data_source").getAsJsonObject());
      }

      if (!sourceConfig.has("meta_source")) {
        throw new IllegalArgumentException("meta_source cannot be null");
      } else {
        mmaConfiguration.metaSource = MetaSourceFactory.fromJson(
            sourceConfig.get("meta_source").getAsJsonObject());
      }
    }

    // Parse ODPS config
    if (!jsonObject.has("odps_config")) {
      throw new IllegalArgumentException("odps_config cannot be null");
    }
    mmaConfiguration.odpsConfig = OdpsConfiguration.fromJson(
        jsonObject.get("odps_config").getAsJsonObject());

    // Parse rules
    if (!jsonObject.has("rules")) {
      mmaConfiguration.rules = parseRules(jsonObject.get("rules").getAsJsonArray());
    }
    return mmaConfiguration;
  }

  private static List<RuleFactory.Rule> parseRules(JsonArray jsonArray) {
    List<RuleFactory.Rule> rules = new LinkedList<>();

    for (int i = 0; i < jsonArray.size(); i++) {
      JsonElement element = jsonArray.get(i);

    }
    return null;
  }

  public String getMigrationId() {
    return migrationId;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public MetaSource getMetaSource() {
    return metaSource;
  }

  public OdpsConfiguration getOdpsConfig() {
    return odpsConfig;
  }

  public List<RuleFactory.Rule> getRules() {
    return null;
  }
}
