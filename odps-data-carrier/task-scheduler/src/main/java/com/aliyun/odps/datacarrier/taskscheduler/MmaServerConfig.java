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

import java.io.IOException;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaServerConfig {
  private static final Logger LOG = LogManager.getLogger(MmaServerConfig.class);

  private static MmaServerConfig instance;

  private DataSource dataSource;
  private MmaConfig.OssConfig ossConfig;
  private MmaConfig.HiveConfig hiveConfig;
  private MmaConfig.OdpsConfig odpsConfig;
  private MmaEventConfig eventConfig;

  MmaServerConfig(DataSource dataSource,
                  MmaConfig.OssConfig ossConfig,
                  MmaConfig.HiveConfig hiveConfig,
                  MmaConfig.OdpsConfig odpsConfig,
                  MmaEventConfig eventConfig) {
    this.dataSource = dataSource;
    this.ossConfig = ossConfig;
    this.hiveConfig = hiveConfig;
    this.odpsConfig = odpsConfig;
    this.eventConfig = eventConfig;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public MmaConfig.OdpsConfig getOdpsConfig() {
    return odpsConfig;
  }

  public MmaConfig.HiveConfig getHiveConfig() {
    return hiveConfig;
  }

  public MmaConfig.OssConfig getOssConfig() {
    return ossConfig;
  }

  public MmaEventConfig getEventConfig() {
    return eventConfig;
  }

  public String toJson() {
    return GsonUtils.getFullConfigGson().toJson(this);
  }

  public boolean validate() {
    boolean valid = true;

    switch (this.dataSource) {
      case Hive:
        if (!this.hiveConfig.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.hiveConfig);
        }
        break;
      case OSS:
        if (!this.ossConfig.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.ossConfig);
        }
        break;
      case ODPS:
        break;
      default:
          throw new IllegalArgumentException("Unsupported datasource");
    }

    if (!odpsConfig.validate()) {
      valid = false;
      LOG.error("Validate MetaConfiguration failed due to {}", this.odpsConfig);
    }

    return valid;
  }

  public static void init(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    MmaServerConfig mmaServerConfig =
        GsonUtils.getFullConfigGson().fromJson(content, MmaServerConfig.class);
    if (mmaServerConfig.validate()) {
      instance = mmaServerConfig;
    } else {
      LOG.error("Invalid MmaServerConfig {}", content);
    }
  }

  public static MmaServerConfig getInstance() {
    if (instance == null) {
      throw new IllegalStateException("MmaServerConfig not initialized");
    }

    return instance;
  }
}
