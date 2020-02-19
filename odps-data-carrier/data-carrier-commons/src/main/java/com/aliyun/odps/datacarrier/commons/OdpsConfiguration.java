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

package com.aliyun.odps.datacarrier.commons;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author: Jon (wangzhong.zw@alibaba-inc.com)
 */
public class OdpsConfiguration {
  private static final String ACCESS_ID = "access_id";
  private static final String ACCESS_KEY = "access_key";
  private static final String ODPS_ENDPOINT = "endpoint";
  private static final String PROJECT_NAME = "project_name";
  private static final String TUNNEL_ENDPOINT = "tunnel_endpoint";

  private String accessId;
  private String accessKey;
  private String endpoint;
  private String projectName;
  private String tunnelEndpoint;

  private OdpsConfiguration () {}

  public static OdpsConfiguration fromJson(JsonObject jsonObject) {
    OdpsConfiguration odpsConfig = new OdpsConfiguration();

    odpsConfig.accessId = JsonUtils.getMemberAsString(jsonObject, ACCESS_ID, true);
    if (StringUtils.isEmpty(odpsConfig.accessId)) {
      throw new IllegalArgumentException("ODPS access ID cannot be empty");
    }

    odpsConfig.accessKey = JsonUtils.getMemberAsString(jsonObject, ACCESS_KEY, true);
    if (StringUtils.isEmpty(odpsConfig.accessKey)) {
      throw new IllegalArgumentException("ODPS access key cannot be empty");
    }

    odpsConfig.endpoint = JsonUtils.getMemberAsString(jsonObject, ODPS_ENDPOINT, true);
    if (StringUtils.isEmpty(odpsConfig.endpoint)) {
      throw new IllegalArgumentException("ODPS endpoint cannot be empty");
    }

    odpsConfig.projectName = JsonUtils.getMemberAsString(jsonObject, PROJECT_NAME, true);
    if (StringUtils.isEmpty(odpsConfig.projectName)) {
      throw new IllegalArgumentException("ODPS project name cannot be empty");
    }

    odpsConfig.tunnelEndpoint = jsonObject.getAsJsonObject().get(TUNNEL_ENDPOINT).getAsString();
    if (StringUtils.isEmpty(odpsConfig.tunnelEndpoint)) {
      odpsConfig.tunnelEndpoint = null;
    }

    return odpsConfig;
  }

  public String getAccessId() {
    return accessId;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getOdpsEndpoint() {
    return endpoint;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  public String getProjectName() {
    return projectName;
  }
}
