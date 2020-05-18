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
  ODPS_DROP_TABLE(0),
  ODPS_CREATE_TABLE(1),
  ODPS_CREATE_EXTERNAL_TABLE(1),
  ODPS_DROP_PARTITION(2),
  ODPS_ADD_PARTITION(3),
  ODPS_ADD_EXTERNAL_TABLE_PARTITION(3),
  ODPS_LOAD_DATA(4),
  HIVE_LOAD_DATA(4),
  // in scenario table migrate from ODPS to ODPS, should validate both source and destination table
  ODPS_SOURCE_VERIFICATION(5),
  ODPS_DESTINATION_VERIFICATION(5),
  HIVE_SOURCE_VERIFICATION(5),
  VERIFICATION(6),
  UNKNOWN(Integer.MAX_VALUE);

  int priority;

  Action(int priority) {
    this.priority = priority;
  }

  public int getPriority() {
    return priority;
  }
}
