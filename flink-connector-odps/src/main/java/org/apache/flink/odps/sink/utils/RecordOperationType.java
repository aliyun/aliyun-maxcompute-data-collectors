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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.utils;

import org.apache.flink.odps.FlinkOdpsException;

import java.util.Locale;

public enum RecordOperationType {

    INSERT("insert"),

    UPSERT("upsert"),

    DELETE("detete"),

    UNKNOWN("unknown");

    private final String value;

    RecordOperationType(String value) {
        this.value = value;
    }

    public static RecordOperationType fromValue(String value) {
        switch (value.toLowerCase(Locale.ROOT)) {
            case "insert":
                return INSERT;
            case "upsert":
                return UPSERT;
            case "detete":
                return DELETE;
            case "unknown":
                return UNKNOWN;
            default:
                throw new FlinkOdpsException("Invalid value of Type.");
        }
    }

    public String value() {
        return value;
    }
}