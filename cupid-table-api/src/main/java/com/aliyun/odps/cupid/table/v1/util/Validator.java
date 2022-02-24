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

package com.aliyun.odps.cupid.table.v1.util;

import java.util.List;
import java.util.Map;

public class Validator {

    public static void checkNotNull(Object arg, String name) {
        if (arg == null) {
            throw new IllegalArgumentException(name + " == null!");
        }
    }

    public static void checkArray(Object[] arg, int minLength, String name) {
        checkNotNull(arg, name);
        if (arg.length < minLength) {
            throw new IllegalArgumentException(name + " must has at least " + minLength + "items");
        }
        for (Object item : arg) {
            checkNotNull(item, name + "[x]");
        }
    }

    public static void checkInteger(Integer arg, int minValue, String name) {
        checkNotNull(arg, name);
        if (arg < minValue) {
            throw new IllegalArgumentException(name + " < " + minValue);
        }
    }

    public static void checkString(String arg, String name) {
        if (arg == null || arg.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty string!");
        }
    }

    public static void checkArray(String[] arg, String name) {
        checkArray(arg, 1, name);
    }

    public static void checkArray(String[] arg, int minLength, String name) {
        checkNotNull(arg, name);
        if (arg.length < minLength) {
            throw new IllegalArgumentException(name + " must has at least " + minLength + "items");
        }
        for (String str : arg) {
            checkString(str, name + "[x]");
        }
    }

    public static void checkMap(Map<String, String> arg, String name) {
        checkMap(arg, 1, name);
    }

    public static void checkMap(Map<String, String> arg, int minSize, String name) {
        checkNotNull(arg, name);
        if (arg.size() < minSize) {
            throw new IllegalArgumentException(name + " must has at least " + minSize + "items");
        }
        for (Map.Entry<String, String> entry : arg.entrySet()) {
            checkString(entry.getKey(), name + " key");
            checkString(entry.getValue(), name + " value");
        }
    }

    public static void checkList(List<?> arg, String name) {
        checkList(arg, 1, name);
    }

    public static void checkList(List<?> arg, int minSize, String name) {
        checkNotNull(arg, name);
        if (arg.size() < minSize) {
            throw new IllegalArgumentException(name + " must has at least " + minSize + "items");
        }
        for (Object item : arg) {
            checkNotNull(item, name + "[x]");
        }
    }

    public static void checkMapList(List<Map<String, String>> arg, String name) {
        checkMapList(arg, 1, 1, name);
    }

    public static void checkMapList(List<Map<String, String>> arg,
                                    int listMinSize,
                                    int mapMinSize,
                                    String name) {
        checkNotNull(arg, name);
        if (arg.size() < listMinSize) {
            throw new IllegalArgumentException(
                    name + " must has at least " + listMinSize + "items");
        }
        for (Map<String, String> map : arg) {
            checkMap(map, mapMinSize, name + "[x]");
        }
    }

    public static void checkIntList(List<Integer> arg,
                                    int minSize,
                                    int minValue,
                                    String name) {
        checkNotNull(arg, name);
        if (arg.size() < minSize) {
            throw new IllegalArgumentException(name + " must has at least " + minSize + "items");
        }
        for (Integer value : arg) {
            checkInteger(value, minValue, name + "[x]");
        }
    }

    public static void checkBucketFileIndices(Map<Integer, List<Integer>> arg,
                                              String name) {
        if (arg == null || arg.isEmpty()) {
            throw new IllegalArgumentException(name + " is null or empty map!");
        }
        for (Map.Entry<Integer, List<Integer>> entry : arg.entrySet()) {
            checkInteger(entry.getKey(), 0, name + " key");
            checkIntList(entry.getValue(), 1, 0, name + " value");
        }
    }
}
