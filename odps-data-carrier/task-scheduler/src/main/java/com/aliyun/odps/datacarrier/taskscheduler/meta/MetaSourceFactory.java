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

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.HiveConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.OdpsConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;

public class MetaSourceFactory {

  public static MetaSource getMetaSource() throws MetaException {
    DataSource dataSource = MmaServerConfig.getInstance().getDataSource();
    if (DataSource.Hive.equals(dataSource)) {
      HiveConfig hiveConfig = MmaServerConfig.getInstance().getHiveConfig();
      return new HiveMetaSource(hiveConfig.getHmsThriftAddr(),
                                hiveConfig.getKrbPrincipal(),
                                hiveConfig.getKeyTab(),
                                hiveConfig.getKrbSystemProperties());
    } else if (DataSource.ODPS.equals(dataSource)) {
      OdpsConfig odpsConfig = MmaServerConfig.getInstance().getOdpsConfig();
      return new OdpsMetaSource(odpsConfig.getAccessId(),
                                odpsConfig.getAccessKey(),
                                odpsConfig.getEndpoint(),
                                odpsConfig.getProjectName());
    } else {
      throw new IllegalArgumentException("Unsupported datasource: " + dataSource);
    }
  }
}
