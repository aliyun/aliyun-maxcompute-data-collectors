//package com.aliyun.odps.datacarrier.commons;
//
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import java.util.function.Function;
//
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//
//public class FilterFactory {
//
//  /**
//   * Operators
//   */
//  public static final String OPERATOR_AND = "and";
//
//
//
//  /**
//   * Table attributes
//   */
//  public static final String TABLE_ATTR_IS_PARTITIONED = "$is_partitioned";
//
//  /**
//   * Predicates
//   */
//  public static final String PREDICATE_EQUALS = "$equals";
//
//  public static <T> Function<T, Boolean> fromJson(String object,
//                                                  JsonObject jsonObject,
//                                                  Class<T> cls) {
//    if (jsonObject.size() > 1) {
//      throw new IllegalArgumentException("Found more than one top level operator");
//    } else if (jsonObject.size() == 0) {
//      return metaModel -> true;
//    }
//
//    Iterator<Map.Entry<String, JsonElement>> it = jsonObject.entrySet().iterator();
//    Map.Entry<String, JsonElement> entry = it.next();
//    return parseOperator(entry.getKey(), entry.getValue(), object, cls);
//  }
//
//  public static <T> Function<T, Boolean> parseOperator(
//      String operator,
//      JsonElement jsonElement,
//      String object,
//      Class<T> cls) {
//
//    if (jsonElement.isJsonObject()) {
//      // Nested operator
//      if (OPERATOR_AND.equalsIgnoreCase(operator)) {
//        return metaModel -> {
//          for (Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
//            // Iterate over each sub operator, return false if any of them returns false
//            if (!parseOperator(entry.getKey(), entry.getValue(), object, cls).apply(metaModel)) {
//              return false;
//            }
//          }
//          // Return true if all of sub operators return true
//          return true;
//        };
//      } else {
//        throw new IllegalArgumentException("Unsupported operator: " + operator);
//      }
//    } else if (jsonElement.isJsonArray()) {
//      if (OPERATOR_AND.equalsIgnoreCase(operator)) {
//        if (Rules.OBJECT_TABLE.equalsIgnoreCase(object)) {
//          return metaModel -> {
//            for (JsonElement element : jsonElement.getAsJsonArray()) {
//              if (!parseTableCondition(element.getAsJsonObject()).apply(
//                  (MetaManager.HiveTableMetaModel) metaModel)) {
//                return false;
//              }
//            }
//            return true;
//          };
//        } else {
//          throw new IllegalArgumentException("Unsupported object: " + object);
//        }
//      } else {
//        throw new IllegalArgumentException("Unsupported operator: " + operator);
//      }
//    } else {
//      throw new IllegalArgumentException(
//          "Malformed operator:\n" + Constants.GSON.toJson(jsonElement));
//    }
//  }
//
//  public static Function<MetaManager.HiveTableMetaModel, Boolean> parseTableCondition(
//      JsonObject jsonObject) {
//    String attribute = JsonUtils.getMemberAsString(jsonObject,
//                                                   "attribute",
//                                                   true);
//    String predicate = JsonUtils.getMemberAsString(jsonObject,
//                                                   "predicate",
//                                                   true);
//    String value = JsonUtils.getMemberAsString(jsonObject,
//                                               "value",
//                                               true);
//    return hiveTableMetaModel -> {
//      if (attribute.equalsIgnoreCase(TABLE_ATTR_IS_PARTITIONED)) {
//        if (predicate.equalsIgnoreCase(PREDICATE_EQUALS)) {
//          Boolean isPartitioned = hiveTableMetaModel.partitionColumnModels.size() != 0;
//          return isPartitioned == Boolean.parseBoolean(value);
//        } else {
//          throw new IllegalArgumentException(
//              "Unsupported predicate: " + predicate + " for " + attribute);
//        }
//      } else {
//        throw new IllegalArgumentException("Unsupported table attribute: " + attribute);
//      }
//    };
//  }
//}
