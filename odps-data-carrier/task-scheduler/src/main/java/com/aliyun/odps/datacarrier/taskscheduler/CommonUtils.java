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

import org.apache.hadoop.hive.metastore.api.MetaException;

// TODO: move to another util class
public class CommonUtils {

  public static RunnerType getRunnerTypeByAction(Action action) {
    switch (action) {
      case ODPS_CREATE_TABLE:
      case ODPS_ADD_PARTITION:
      case ODPS_CREATE_EXTERNAL_TABLE:
      case ODPS_ADD_EXTERNAL_TABLE_PARTITION:
      case ODPS_LOAD_DATA:
      case ODPS_SOURCE_VERIFICATION:
      case ODPS_DESTINATION_VERIFICATION:
        return RunnerType.ODPS;
      case HIVE_LOAD_DATA:
      case HIVE_SOURCE_VERIFICATION:
        return RunnerType.HIVE;
      case VERIFICATION:
        return RunnerType.VERIFICATION;
      case UNKNOWN:
      default:
        throw new RuntimeException("Unknown action: " + action.name());
    }
  }

  public static MetaSource getMetaSource(MmaServerConfig config) throws MetaException {
    DataSource dataSource = config.getDataSource();
    if (DataSource.Hive.equals(dataSource)) {
      MmaConfig.HiveConfig hiveConfig = config.getHiveConfig();
      return new HiveMetaSource(hiveConfig.getHmsThriftAddr(),
          hiveConfig.getKrbPrincipal(),
          hiveConfig.getKeyTab(),
          hiveConfig.getKrbSystemProperties());
    } else if (DataSource.ODPS.equals(dataSource)) {
      MmaConfig.OdpsConfig odpsConfig = config.getOdpsConfig();
      return new OdpsMetaSource(odpsConfig.getAccessId(),
          odpsConfig.getAccessKey(),
          odpsConfig.getEndpoint(),
          odpsConfig.getProjectName());
    } else {
      throw new IllegalArgumentException("Unsupported datasource: " + dataSource);
    }
  }
}
