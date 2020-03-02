package com.aliyun.odps.datacarrier.taskscheduler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Modifier;

public class GsonUtils {
  private static final Gson FULL_CONFIG_GSON = new GsonBuilder().
      excludeFieldsWithModifiers(Modifier.STATIC, Modifier.VOLATILE).
      disableHtmlEscaping().setPrettyPrinting().create();

  public static Gson getFullConfigGson() {
    return FULL_CONFIG_GSON;
  }
}
