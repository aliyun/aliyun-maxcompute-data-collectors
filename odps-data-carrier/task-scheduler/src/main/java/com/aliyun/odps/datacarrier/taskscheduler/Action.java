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

public enum Action {
  ODPS_CREATE_TABLE(1),
  ODPS_CREATE_EXTERNAL_TABLE(1),
  ODPS_ADD_PARTITION(2),
  ODPS_ADD_EXTERNAL_TABLE_PARTITION(2),
  ODPS_LOAD_DATA(3),
  HIVE_LOAD_DATA(3),
  ODPS_VERIFICATION(4),
  HIVE_VERIFICATION(4),
  VERIFICATION(5),
  UNKNOWN(Integer.MAX_VALUE);

  int priority;
  Action(int priority) {
    this.priority = priority;
  }

  public int getPriority() {
    return priority;
  }
}
