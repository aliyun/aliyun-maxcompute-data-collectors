package com.aliyun.odps.datacarrier.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;

public class RuleFactory {
  /**
   * Objects
   */
  public static final String OBJECT_TABLE = "table";

  public static class Rule<S, T> {
    private String object;
    private Function<S, Boolean> filter;
    private ActionFactory.Action<T> action;
    private Map<String, String> config;
    private void apply(S metaModel, T odpsMetaModel) {
      if (filter.apply(metaModel)) {
        action.apply(odpsMetaModel, config);
      }
    }
  }

  public static Rule fromJson(JsonObject jsonObject, MetaSource metaSource) {
//    Rule<MetaManager., MetaManager.OdpsTableMetaModel> rule = new Rule<>();

    String object = JsonUtils.getMemberAsString(jsonObject, "object", true);
    if (StringUtils.isBlank(object)) {
      throw new IllegalArgumentException("object cannot be empty");
    }
//    rule.object = object;

    //TODO: remove
    return null;
  }
}
