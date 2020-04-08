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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Modifier;

// TDOO: move to constants
public class GsonUtils {
  private static final Gson FULL_CONFIG_GSON = new GsonBuilder().
      excludeFieldsWithModifiers(Modifier.STATIC, Modifier.VOLATILE).
      disableHtmlEscaping().setPrettyPrinting().create();

  public static Gson getFullConfigGson() {
    return FULL_CONFIG_GSON;
  }

  public static String toJson(Object object) {
    return FULL_CONFIG_GSON.toJson(object);
  }
}
