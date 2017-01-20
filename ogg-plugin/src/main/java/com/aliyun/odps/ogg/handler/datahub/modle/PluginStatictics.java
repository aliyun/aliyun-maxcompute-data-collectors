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

package com.aliyun.odps.ogg.handler.datahub.modle;

/**
 * Created by lyf0429 on 16/5/16.
 */
public class PluginStatictics {
    private static long totalInserts = 0L;
    private static long totalUpdates = 0L;
    private static long totalDeletes =  0L;
    private static long totalTxns = 0L;
    private static long totalOperations = 0L;
    private static long sendTimesInTx = 0L;

    public static long getTotalInserts() {
        return totalInserts;
    }

    public static void setTotalInserts(long totalInserts) {
        PluginStatictics.totalInserts = totalInserts;
    }

    public static void addTotalInserts() {
        totalInserts++;
    }

    public static long getTotalUpdates() {
        return totalUpdates;
    }

    public static void setTotalUpdates(long totalUpdates) {
        PluginStatictics.totalUpdates = totalUpdates;
    }

    public static void addTotalUpdates() {
        totalUpdates++;
    }

    public static long getTotalDeletes() {
        return totalDeletes;
    }

    public static void setTotalDeletes(long totalDeletes) {
        PluginStatictics.totalDeletes = totalDeletes;
    }

    public static void addTotalDeletes() {
        totalDeletes++;
    }

    public static long getTotalTxns() {
        return totalTxns;
    }

    public static void setTotalTxns(long totalTxns) {
        PluginStatictics.totalTxns = totalTxns;
    }

    public static void addTotalTxns() {
        totalTxns++;
    }

    public static long getTotalOperations() {
        return totalOperations;
    }

    public static void setTotalOperations(long totalOperations) {
        PluginStatictics.totalOperations = totalOperations;
    }

    public static void addTotalOperations() {
        totalOperations++;
    }

    public static long getSendTimesInTx() {
        return sendTimesInTx;
    }

    public static void setSendTimesInTx(long sendTimesInTx) {
        PluginStatictics.sendTimesInTx = sendTimesInTx;
    }

    public static void addSendTimesInTx() {
        sendTimesInTx++;
    }
}
