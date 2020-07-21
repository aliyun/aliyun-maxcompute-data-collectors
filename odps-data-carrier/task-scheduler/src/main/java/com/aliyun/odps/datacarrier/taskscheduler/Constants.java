/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

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
  public static final String DB_FILE_NAME = ".MmaMeta";

  // Types
  public static final String VARCHAR_255 = "VARCHAR(255)";
  public static final String VARCHAR_65535 = "VARCHAR(65535)";
  public static final String INT = "INT";
  public static final String BIGINT = "BIGINT";
  public static final String BOOLEAN = "BOOLEAN";

  /**
   * Schema: default, table: MMA_TBL_META
   */
  public static final String MMA_TBL_META_TBL_NAME = "MMA_TBL_META";
  public static final String MMA_TBL_META_COL_DB_NAME = "db_name";
  public static final String MMA_TBL_META_COL_TBL_NAME = "table_name";
  public static final String MMA_TBL_META_COL_IS_PARTITIONED = "is_partitioned";
  public static final String MMA_TBL_META_COL_MIGRATION_CONF = "migration_config";
  public static final String MMA_TBL_META_COL_STATUS = "status";
  public static final String MMA_TBL_META_COL_ATTEMPT_TIMES = "attempt_times";
  public static final String MMA_TBL_META_COL_LAST_MODIFIED_TIME = "last_modified_time";
  public static final Map<String, String> MMA_TBL_META_COL_TO_TYPE;
  static {
    Map<String, String> temp = new LinkedHashMap<>();
    temp.put(MMA_TBL_META_COL_DB_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_TBL_NAME, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_IS_PARTITIONED, BOOLEAN);
    temp.put(MMA_TBL_META_COL_MIGRATION_CONF, VARCHAR_65535);
    temp.put(MMA_TBL_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_TBL_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_TBL_META_COL_LAST_MODIFIED_TIME, BIGINT);
    MMA_TBL_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);
  }

  /**
   * Init value for column 'attempt_times'
   */
  public static final int MMA_TBL_META_INIT_VALUE_ATTEMPT_TIMES = 0;
  /**
   * N/A value for column 'last_modified_time'
   */
  public static final long MMA_TBL_META_NA_VALUE_LAST_MODIFIED_TIME = -1L;

  /**
   * Schema: MMA_PT_META_DB_[db], table: MMA_PT_META_TBL_[tbl]
   */
  public static final String MMA_PT_META_SCHEMA_NAME_FMT = "MMA_PT_META_DB_%s";
  public static final String MMA_PT_META_SCHEMA_NAME_PREFIX = "MMA_PT_META_DB_";
  public static final String MMA_PT_META_TBL_NAME_FMT = "MMA_PT_META_TBL_%s";
  public static final String MMA_PT_META_COL_PT_VALS = "pt_vals";
  public static final String MMA_PT_META_COL_STATUS = "status";
  public static final String MMA_PT_META_COL_ATTEMPT_TIMES = "attempt_times";
  public static final String MMA_PT_META_COL_LAST_MODIFIED_TIME = "last_modified_time";
  public static final Map<String, String> MMA_PT_META_COL_TO_TYPE;
  static {
    Map<String, String> temp = new LinkedHashMap<>();
    temp.put(MMA_PT_META_COL_PT_VALS, VARCHAR_65535);
    temp.put(MMA_PT_META_COL_STATUS, VARCHAR_255);
    temp.put(MMA_PT_META_COL_ATTEMPT_TIMES, INT);
    temp.put(MMA_PT_META_COL_LAST_MODIFIED_TIME, BIGINT);
    MMA_PT_META_COL_TO_TYPE = Collections.unmodifiableMap(temp);
  }

  /**
   * Init value for column 'attempt_times'
   */
  public static final int MMA_PT_META_INIT_ATTEMPT_TIMES = 0;
  /**
   * N/A value for column 'last modified time'
   */
  public static final long MMA_PT_MEAT_NA_LAST_MODIFIED_TIME_ = -1L;

  // OSS export folder/file names
  public static final String OSS_ROOT_FOLDER_NAME = "ODPS_MMA/";
  public static final String EXPORT_FUNCTION_DDL_FOLDER_NAME = "export_functions_ddl/";
  public static final String EXPORT_RESOURCE_DDL_FOLDER_NAME = "export_resources_ddl/";
  public static final String EXPORT_RESOURCE_OBJECT_FOLDER_NAME = "export_resources_objects/";
  public static final String EXPORT_TABLE_DDL_FOLDER_NAME = "export_tables_ddl/";
  public static final String EXPORT_DDL_FILE_NAME = "DDL";
}
