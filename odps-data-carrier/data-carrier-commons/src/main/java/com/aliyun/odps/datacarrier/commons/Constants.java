package com.aliyun.odps.datacarrier.commons;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Constants {
  public static final String DEFAULT_CHARSET = "utf-8";

  public static final Gson GSON =
      new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

  public enum DATASOURCE_TYPE {
    /**
     * hive
     */
    HIVE,
    /**
     * mysql
     */
    MYSQL
  }
}
