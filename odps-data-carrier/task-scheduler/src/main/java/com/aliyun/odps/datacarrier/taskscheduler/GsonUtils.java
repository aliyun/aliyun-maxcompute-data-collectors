package com.aliyun.odps.datacarrier.taskscheduler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Modifier;

public class GsonUtils {
  private static Gson fullConfigGson = new GsonBuilder().
      excludeFieldsWithModifiers(Modifier.STATIC, Modifier.VOLATILE).
      disableHtmlEscaping().setPrettyPrinting().create();

  public static Gson getFullConfigGson() {
    return fullConfigGson;
  }
}
