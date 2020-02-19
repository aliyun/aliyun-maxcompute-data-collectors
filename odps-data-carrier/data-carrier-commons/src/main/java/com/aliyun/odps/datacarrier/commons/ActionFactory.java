package com.aliyun.odps.datacarrier.commons;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class ActionFactory {

  public static final String ACTION_CHANGE_COLUMN_TYPE = "$change_column_type";

  public static Type CONFIG_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  public interface Action<T> {
    public void apply(T metaModel, Map<String, String> config);

    public void validateConfig(Map<String, String> config);

    public boolean requiresConfig();

    public String usage();
  }

  public static Map<String, Action> nameToAction = new HashMap<>();
  static {
//    nameToAction.put(ACTION_CHANGE_COLUMN_TYPE, new ChangeColumnType());
  }

  public static Action fromJson(JsonObject jsonObject) {
    String actionName = JsonUtils.getMemberAsString(jsonObject,
                                                    "name",
                                                    true);
    if (!nameToAction.containsKey(actionName)) {
      throw new IllegalArgumentException("Unsupported action: " + actionName);
    }
    return nameToAction.get(actionName);

//    if (action.requiresConfig()) {
//      Map<String, String> config = JsonUtils.getMemberAsMap(jsonObject,
//                                                            "config",
//                                                            true,
//                                                            String.class,
//                                                            String.class);
//      action.validateConfig(config);
//    }
  }

//  public static class ChangeColumnType implements Action<MetaManager.OdpsTableMetaModel> {
//    private static String CONFIG_COLUMN_NAME = "column_name";
//    private static String CONFIG_TARGET_TYPE = "target_type";
//
//    @Override
//    public void apply(MetaManager.OdpsTableMetaModel tableMetaModel,
//                      Map<String, String> config) {
//      for (MetaManager.ColumnMetaModel columnMetaModel : tableMetaModel.columnMetaModels) {
//        if (config.get(CONFIG_COLUMN_NAME).equalsIgnoreCase(columnMetaModel.columnName)) {
//          columnMetaModel.type = config.get(CONFIG_TARGET_TYPE);
//        }
//      }
//    }
//
//    @Override
//    public void validateConfig(Map<String, String> config) {
//      if (config.get(CONFIG_COLUMN_NAME) == null ||
//          StringUtils.isBlank(config.get(CONFIG_COLUMN_NAME))) {
//        throw new IllegalArgumentException(
//            "Action " + ACTION_CHANGE_COLUMN_TYPE + " requires " + CONFIG_COLUMN_NAME);
//      }
//      if (config.get(CONFIG_TARGET_TYPE) == null ||
//          StringUtils.isBlank(config.get(CONFIG_TARGET_TYPE))) {
//        throw new IllegalArgumentException(
//            "Action " + ACTION_CHANGE_COLUMN_TYPE + " requires " + CONFIG_TARGET_TYPE);
//      }
//    }
//
//    @Override
//    public boolean requiresConfig() {
//      return true;
//    }
//
//    @Override
//    public String usage() {
//      return String.format("%s\nconfig:\n\t%s <-- %s\n\t%s <-- %s\n",
//                           ACTION_CHANGE_COLUMN_TYPE,
//                           CONFIG_COLUMN_NAME, "column to change type",
//                           CONFIG_TARGET_TYPE, "type in ODPS");
//    }
//  }
}
