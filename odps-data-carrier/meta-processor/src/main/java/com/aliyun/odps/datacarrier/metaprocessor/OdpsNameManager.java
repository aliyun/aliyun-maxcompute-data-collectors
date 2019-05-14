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

package com.aliyun.odps.datacarrier.metaprocessor;

import com.aliyun.odps.datacarrier.commons.risk.Risk;
import java.util.HashMap;
import java.util.Map;


public class OdpsNameManager {
  private Map<String, String> odpsTableToHiveTable = new HashMap<>();

  public Risk add(String hiveDatabaseName, String odpsProjectName, String hiveTableName,
      String odpsTableName) {
    String key = odpsProjectName + "." + odpsTableName;
    if (odpsTableToHiveTable.containsKey(key)) {
      String[] value = odpsTableToHiveTable.get(key).split("\\.");
      return Risk.getTableNameConflictRisk(value[0], value[1], hiveDatabaseName, hiveTableName);
    } else {
      odpsTableToHiveTable.put(key, hiveDatabaseName + "." + hiveTableName);
      return Risk.getNoRisk();
    }
  }

  public Map<String, String> getHiveTableToOdpsTableMap() {
    Map<String, String> hiveTableToOdpsTable = new HashMap<>();
    for (String key : odpsTableToHiveTable.keySet()) {
      hiveTableToOdpsTable.put(odpsTableToHiveTable.get(key), key);
    }

    return hiveTableToOdpsTable;
  }
}
