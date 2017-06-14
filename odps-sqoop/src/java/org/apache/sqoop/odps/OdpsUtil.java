/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.odps;

import java.util.Random;

/**
 * Created by Tian Li on 15/9/29.
 */
public class OdpsUtil {
  private static boolean testingMode = false;
  private static final String INSTANCE_CLASS = "com.aliyun.odps.Odps";

  private OdpsUtil() {
  }

  /**
   * This is a way to make this always return false for testing.
   */
  public static void setAlwaysNoOdpsJarMode(boolean mode) {
    testingMode = mode;
  }

  public static boolean isOdpsJarPresent() {
    if (testingMode) {
      return false;
    }
    try {
      Class.forName(INSTANCE_CLASS);
    } catch (ClassNotFoundException e) {
      return false;
    }
    return true;
  }

  public static String getUserAgent() {
    return "odps-sqoop-1.4.6";
  }

  //tunnel endpoint is split by ";", like http://100.100.100.100|http://100.100.100.101
  public static String getTunnelEndPoint(String tunnelEndpoints) {
    String[] endpoints = tunnelEndpoints.split("|");
    Random rand = new Random();
    int index = rand.nextInt(endpoints.length);
    return endpoints[index];
  }
}
