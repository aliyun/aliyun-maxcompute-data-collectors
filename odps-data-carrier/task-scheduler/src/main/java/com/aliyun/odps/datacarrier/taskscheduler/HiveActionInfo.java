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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class HiveActionInfo extends AbstractActionInfo {

  private String jobId;
  private String trackingUrl;
  private List<List<String>> result;

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  public void setResult(List<List<String>> result) {
    this.result = result;
  }

  public List<List<String>> getResult() {
    return result;
  }

  public String getHiveActionInfoSummary() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\nDatetime: ").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    sb.append("\nJobId=").append(jobId);
    sb.append("\nTrackingUrl=").append(trackingUrl);
    sb.append("\n");
    return sb.toString();
  }
}
