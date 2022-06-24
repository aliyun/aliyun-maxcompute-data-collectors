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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lyf0429 on 16/5/15.
 */
public class Configure {
    private String oracleSid;
    private String datahubEndpoint;
    private String datahubAccessId;
    private String datahubAccessKey;
    private boolean enablePb = true;
    private String compressType = "LZ4";

    // for send
    private int batchSize = 1000;
    private int batchTimeoutMs = 5000;
    //for build
    private int buildBatchSize = 10;
    private int buildBatchTimeoutMs = 100;

    private boolean dirtyDataContinue = false;
    private String dirtyDataFile = "datahub_ogg_plugin.dirty";
    private int dirtyDataFileMaxSize = 500 * 1000000;

    private int retryTimes = -1;
    private int retryIntervalMs = 3000;

    private Map<String, TableMapping> tableMappings;

    private int buildRecordQueueSize = 1024;
    private int buildRecordQueueTimeoutMs = 1000;
    private int writeRecordQueueSize = 1024;
    private int writeRecordQueueTimeoutMs = 1000;

    private boolean recordAccess = true;
    private boolean reportMetric = false;
    private int reportMetricIntervalMs = 5 * 60 * 1000;
    private int buildRecordCorePoolSize = -1;
    private int buildRecordMaximumPoolSize = -1;
    private int writeRecordCorePoolSize = -1;
    private int writeRecordMaximumPoolSize = -1;

    public String getOracleSid() {
        return oracleSid;
    }

    public void setOracleSid(String oracleSid) {
        this.oracleSid = oracleSid;
    }

    public String getDatahubEndpoint() {
        return datahubEndpoint;
    }

    public void setDatahubEndpoint(String datahubEndpoint) {
        this.datahubEndpoint = datahubEndpoint;
    }

    public String getDatahubAccessId() {
        return datahubAccessId;
    }

    public void setDatahubAccessId(String datahubAccessId) {
        this.datahubAccessId = datahubAccessId;
    }

    public String getDatahubAccessKey() {
        return datahubAccessKey;
    }

    public void setDatahubAccessKey(String datahubAccessKey) {
        this.datahubAccessKey = datahubAccessKey;
    }

    public boolean isEnablePb() {
        return enablePb;
    }

    public void setEnablePb(boolean enablePb) {
        this.enablePb = enablePb;
    }

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = Math.min(Math.max(batchSize, 0), 1000);
    }

    public int getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public Configure setBatchTimeoutMs(int batchTimeoutMs) {
        this.batchTimeoutMs = batchTimeoutMs;
        return this;
    }

    public int getBuildBatchSize() {
        return buildBatchSize;
    }

    public void setBuildBatchSize(int buildBatchSize) {
        this.buildBatchSize = buildBatchSize;
    }

    public int getBuildBatchTimeoutMs() {
        return buildBatchTimeoutMs;
    }

    public void setBuildBatchTimeoutMs(int buildBatchTimeoutMs) {
        this.buildBatchTimeoutMs = buildBatchTimeoutMs;
    }

    public boolean isDirtyDataContinue() {
        return dirtyDataContinue;
    }

    public void setDirtyDataContinue(boolean dirtyDataContinue) {
        this.dirtyDataContinue = dirtyDataContinue;
    }

    public String getDirtyDataFile() {
        return dirtyDataFile;
    }

    public void setDirtyDataFile(String dirtyDataFile) {
        this.dirtyDataFile = dirtyDataFile;
    }

    public int getDirtyDataFileMaxSize() {
        return dirtyDataFileMaxSize;
    }

    public void setDirtyDataFileMaxSize(int dirtyDataFileMaxSize) {
        this.dirtyDataFileMaxSize = dirtyDataFileMaxSize * 1000000;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getRetryIntervalMs() {
        return retryIntervalMs;
    }

    public void setRetryIntervalMs(int retryIntervalMs) {
        this.retryIntervalMs = retryIntervalMs;
    }

    public Map<String, TableMapping> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(Map<String, TableMapping> tableMappings) {
        this.tableMappings = tableMappings;
    }

    public void addTableMapping(TableMapping tableMapping) {
        if (this.tableMappings == null) {
            this.tableMappings = new HashMap<>();
        }

        this.tableMappings.put(tableMapping.getOracleFullTableName(), tableMapping);
    }

    public TableMapping getTableMapping(String oracleFullTableName) {
        return this.tableMappings.get(oracleFullTableName);
    }

    public int getBuildRecordQueueSize() {
        return buildRecordQueueSize;
    }

    public Configure setBuildRecordQueueSize(int buildRecordQueueSize) {
        this.buildRecordQueueSize = buildRecordQueueSize;
        return this;
    }

    public int getBuildRecordQueueTimeoutMs() {
        return buildRecordQueueTimeoutMs;
    }

    public Configure setBuildRecordQueueTimeoutMs(int buildRecordQueueTimeoutMs) {
        this.buildRecordQueueTimeoutMs = buildRecordQueueTimeoutMs;
        return this;
    }

    public int getWriteRecordQueueSize() {
        return writeRecordQueueSize;
    }

    public void setWriteRecordQueueSize(int writeRecordQueueSize) {
        this.writeRecordQueueSize = writeRecordQueueSize;
    }

    public int getWriteRecordQueueTimeoutMs() {
        return writeRecordQueueTimeoutMs;
    }

    public void setWriteRecordQueueTimeoutMs(int writeRecordQueueTimeoutMs) {
        this.writeRecordQueueTimeoutMs = writeRecordQueueTimeoutMs;
    }

    public boolean isRecordAccess() {
        return recordAccess;
    }

    public void setRecordAccess(boolean recordAccess) {
        this.recordAccess = recordAccess;
    }

    public boolean isReportMetric() {
        return reportMetric;
    }

    public void setReportMetric(boolean reportMetric) {
        this.reportMetric = reportMetric;
    }

    public int getReportMetricIntervalMs() {
        return reportMetricIntervalMs;
    }

    public void setReportMetricIntervalMs(int reportMetricIntervalMs) {
        this.reportMetricIntervalMs = reportMetricIntervalMs;
    }

    public int getBuildRecordCorePoolSize() {
        return buildRecordCorePoolSize;
    }

    public void setBuildRecordCorePoolSize(int buildRecordCorePoolSize) {
        this.buildRecordCorePoolSize = buildRecordCorePoolSize;
    }

    public int getBuildRecordMaximumPoolSize() {
        return buildRecordMaximumPoolSize;
    }

    public void setBuildRecordMaximumPoolSize(int buildRecordMaximumPoolSize) {
        this.buildRecordMaximumPoolSize = buildRecordMaximumPoolSize;
    }

    public int getWriteRecordCorePoolSize() {
        return writeRecordCorePoolSize;
    }

    public void setWriteRecordCorePoolSize(int writeRecordCorePoolSize) {
        this.writeRecordCorePoolSize = writeRecordCorePoolSize;
    }

    public int getWriteRecordMaximumPoolSize() {
        return writeRecordMaximumPoolSize;
    }

    public void setWriteRecordMaximumPoolSize(int writeRecordMaximumPoolSize) {
        this.writeRecordMaximumPoolSize = writeRecordMaximumPoolSize;
    }
}
