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

public class ColorsGenerator {

  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_BLACK = "\u001B[30m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_YELLOW = "\u001B[33m";
  private static final String ANSI_BLUE = "\u001B[34m";
  private static final String ANSI_PURPLE = "\u001B[35m";
  private static final String ANSI_CYAN = "\u001B[36m";
  private static final String ANSI_WHITE = "\u001B[37m";


  public static String printRed(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_RED);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printGreen(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_GREEN);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printYellow(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_YELLOW);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }

  public static String printWhite(String inputStr) {
    StringBuilder sb = new StringBuilder(ANSI_WHITE);
    sb.append(inputStr).append(ANSI_RESET);
    return sb.toString();
  }
}
