package com.aliyun.odps.datacarrier.commons;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

public class JsonUtils {
  public static String getMemberAsString(JsonObject jsonObject,
                                         String memberName,
                                         boolean throwIfAbsent) {
    JsonElement member = jsonObject.get(memberName);
    if (member == null && throwIfAbsent) {
      throw new IllegalArgumentException(memberName + " cannot be null");
    }

    return member == null ? null : member.getAsString();
  }

  public static String[] getMemberAsStringArray(JsonObject jsonObject,
                                                String memberName,
                                                boolean throwIfAbsent) {
    JsonElement member = jsonObject.get(memberName);
    if (member == null && throwIfAbsent) {
      throw new IllegalArgumentException(memberName + " cannot be null");
    }

    if (member != null) {
      JsonArray jsonArray = member.getAsJsonArray();
      String[] ret = new String[jsonArray.size()];

      for (int i = 0; i < jsonArray.size(); i++) {
        ret[i] = jsonArray.get(i).getAsString();
      }

      return ret;
    } else {
      return null;
    }
  }

  public static <K, V> Map<K, V> getMemberAsMap(JsonObject jsonObject,
                                                String memberName,
                                                boolean throwIfAbsent,
                                                Class<K> k,
                                                Class<V> v) {
    JsonElement member = jsonObject.get(memberName);
    if (member == null && throwIfAbsent) {
      throw new IllegalArgumentException(memberName + " cannot be null");
    }

    if (member != null) {
      return Constants.GSON.fromJson(member, new TypeToken<Map<K, V>>() {}.getType());
    } else {
      return null;
    }
  }
}
