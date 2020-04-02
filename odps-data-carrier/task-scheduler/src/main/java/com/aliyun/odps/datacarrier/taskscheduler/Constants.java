package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class Constants {

  public static final String HELP = "help";
  public static final String PROJECT_NAME = "project_name";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";
  public static final String END_POINT = "end_point";
  public static final String TUNNEL_ENDPOINT = "tunnel_endpoint";

  /*
    Constants of MmaMetaManagerDbImpl
   */
  protected static final String DB_FILE_NAME = ".MmaMeta";

  // Types
  public static final String VARCHAR_255 = "VARCHAR(255)";
  public static final String VARCHAR_65535 = "VARCHAR(65535)";
  public static final String INT = "INT";
  public static final String BIGINT = "BIGINT";
  public static final String BOOLEAN = "BOOLEAN";

  // Schema: default, table: MMA_META
  public static final String MMA_TBL_META_TBL_NAME = "MMA_TBL_META";
  public static final String MMA_TBL_META_COL_DB_NAME = "db_name";
  public static final String MMA_TBL_META_COL_TBL_NAME = "table_name";
  public static final String MMA_TBL_META_COL_IS_PARTITIONED = "is_partitioned";
  public static final String MMA_TBL_META_COL_MIGRATION_CONF = "migration_config";
  public static final String MMA_TBL_META_COL_STATUS = "status";
  public static final String MMA_TBL_META_COL_ATTEMPT_TIMES = "attempt_times";
  public static final String MMA_TBL_META_COL_LAST_SUCC_TIMESTAMP = "last_succ_timestamp";
  public static final Map<String, String> MMA_TBL_META_COL_TO_TYPE;
  static {
    Map<String, String> temp = new LinkedHashMap<>();
    temp.put(MMA_TBL_META_COL_DB_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_TBL_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_IS_PARTITIONED, BOOLEAN);
    temp.put(MMA_TBL_META_COL_MIGRATION_CONF, VARCHAR_65535);
    temp.put(MMA_TBL_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_TBL_META_COL_LAST_SUCC_TIMESTAMP, BIGINT);
    MMA_TBL_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);
  }
  // Constant values
  public static final int MMA_TBL_META_INIT_ATTEMPT_TIMES = 0;
  public static final long MMA_TBL_META_INIT_LAST_SUCC_TIMESTAMP = -1;

  // Schema: [db], table: [tbl],
  public static final String MMA_PT_META_SCHEMA_NAME_FMT = "MMA_PT_META_DB_%s";
  public static final String MMA_PT_META_SCHEMA_NAME_PREFIX = "MMA_PT_META_DB_";
  public static final String MMA_PT_META_TBL_NAME_FMT = "MMA_PT_META_TBL_%s";
  public static final String MMA_PT_META_COL_PT_VALS = "pt_vals";
  public static final String MMA_PT_META_COL_STATUS = "status";
  public static final String MMA_PT_META_COL_ATTEMPT_TIMES = "attempt_times";
  public static final String MMA_PT_META_COL_LAST_SUCC_TIMESTAMP = "last_succ_timestamp";
  public static final Map<String, String> MMA_PT_META_COL_TO_TYPE;
  static {
    Map<String, String> temp = new LinkedHashMap<>();
    temp.put(MMA_PT_META_COL_PT_VALS, VARCHAR_65535);
    temp.put(MMA_PT_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_PT_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_PT_META_COL_LAST_SUCC_TIMESTAMP, BIGINT);
    MMA_PT_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);
  }
  // Constant values
  public static final int MMA_PT_META_INIT_ATTEMPT_TIMES = 0;
  public static final long MMA_PT_META_INIT_LAST_SUCC_TIMESTAMP = -1;
}
