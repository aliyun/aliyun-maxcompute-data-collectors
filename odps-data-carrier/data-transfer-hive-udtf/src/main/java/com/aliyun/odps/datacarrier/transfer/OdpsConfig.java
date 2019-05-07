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

package com.aliyun.odps.datacarrier.transfer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class OdpsConfig {
  private static final String ACCESS_ID = "access_id";
  private static final String ACCESS_KEY = "access_key";
  private static final String ODPS_ENDPOINT = "end_point";
  private static final String TUNNEL_ENDPOINT = "tunnel_endpoint";
  private static final String PROJECT_NAME = "project_name";

  private Properties properties;

  public OdpsConfig(String fileName) throws IOException {
    this.properties = new Properties();
    this.properties.load(new FileInputStream(fileName));
  }

  public String getAccessId() {
    return this.properties.getProperty(ACCESS_ID);
  }

  public String getAccessKey() {
    return this.properties.getProperty(ACCESS_KEY);
  }

  public String getOdpsEndpoint() {
    return this.properties.getProperty(ODPS_ENDPOINT);
  }

  public String getTunnelEndpoint() {
    return this.properties.getProperty(TUNNEL_ENDPOINT);
  }

  public String getProjectName() {
    return this.properties.getProperty(PROJECT_NAME);
  }
}
