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

public class Risk {

  public enum RISK_LEVEL {
    /**
     * There are risks that must be taken care of manually
     */
    HIGH,
    /**
     * There are risks, but can be solved automatically
     */
    MODERATE,
    /**
     * No risk
     */
    LOW
  }

  private RISK_LEVEL riskLevel;
  private String description;

  public Risk(RISK_LEVEL riskLevel, String description) {
    if (riskLevel == null) {
      throw new IllegalArgumentException("Risk level cannot be null.");
    }
    this.riskLevel = riskLevel;
    this.description = description;
  }

  public RISK_LEVEL getRiskLevel() {
    return riskLevel;
  }

  public String getDescription() {
    return this.description;
  }

  public static Risk getUnsupportedTypeRisk(String originalType) {
    String description = "Unsupported type: " + originalType + ", have to be handled manually";
    return new Risk(RISK_LEVEL.HIGH, description);
  }

  public static Risk getInCompatibleTypeRisk(String originalType, String transformedType,
      String reason) {
    String description = "Incompatible type \'" + originalType + "\', can be transform to \'" +
        transformedType + "\' automatically, but may cause problem. Reason: " + reason;
    return new Risk(RISK_LEVEL.MODERATE, description);
  }

  public static Risk getTableNameConflictRisk(String firstDatabaseName, String firstTableName,
      String secondDatabaseName, String secondTableName) {
    String description = "Table name conflict: " + firstDatabaseName + "." + firstTableName +
        " is mapped to the same odps table as " + secondDatabaseName + "." + secondTableName;
    return new Risk(RISK_LEVEL.HIGH, description);
  }

  public static Risk getNoRisk() {
    return new Risk(RISK_LEVEL.LOW, null);
  }
}
