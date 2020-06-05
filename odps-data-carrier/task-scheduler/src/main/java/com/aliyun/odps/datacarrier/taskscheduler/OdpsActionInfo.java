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
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.data.Record;

public class OdpsActionInfo extends AbstractActionInfo {
  private String instanceId;
  private String logView;
  private List<List<String>> result;

  public String getInstanceId() {
    return instanceId;
  }

  public String getLogView() {
    return logView;
  }

  public List<List<String>> getResult() {
    return result;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public void setLogView(String logView) {
    this.logView = logView;
  }

  public void setResult(List<Record> records) {
    result = new LinkedList<>();
    for(Record r : records) {
      List<String> row = new LinkedList<>();
      for (int i = 0; i < r.getColumnCount(); i++) {
        row.add(r.getString(i));
      }

      result.add(row);
    }
  }

  public String getOdpsActionInfoSummary() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\nDatetime: ").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
    sb.append("\nInstanceId= ").append(instanceId);
    sb.append("\nLogView= ").append(logView);
    sb.append("\n");
    return sb.toString();
  }
}
