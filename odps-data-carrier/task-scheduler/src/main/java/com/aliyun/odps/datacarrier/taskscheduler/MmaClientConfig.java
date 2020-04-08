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

public class MmaClientConfig extends MmaServerConfig {
  private static final Logger LOG = LogManager.getLogger(MmaClientConfig.class);

  public MmaClientConfig(DataSource dataSource,
                         MmaConfig.OssConfig ossConfig,
                         MmaConfig.HiveConfig hiveConfig,
                         MmaConfig.OdpsConfig odpsConfig) {
    super(dataSource, ossConfig, hiveConfig, odpsConfig);
  }

  @Override
  public boolean validate() {
    boolean valid = true;

    switch (getDataSource()) {
      case Hive:
        if (getHiveConfig() == null || !getHiveConfig().validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", getHiveConfig());
        }
        break;
      case OSS:
        if (getOssConfig() == null || !getOssConfig().validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", getOssConfig());
        }
        break;
    }

    return valid;
  }

  public static MmaClientConfig fromFile(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    return GsonUtils.getFullConfigGson().fromJson(content, MmaClientConfig.class);
  }
}
