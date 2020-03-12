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

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by lyf0429 on 16/5/15.
 */
public class Configure {
    private String oracleSid;

    private String datahubEndpoint;

    private String datahubAccessId;

    private String datahubAccessKey;

    private boolean enablePb = false;

    private String compressType;

    private int batchSize = 1000;

    private boolean dirtyDataContinue = false;

    private String dirtyDataFile = "datahub_ogg_plugin.dirty";

    private int dirtyDataFileMaxSize = 500 * 1000000;

    private int retryTimes = -1;

    private int retryInterval = 3000;

    private String checkPointFileName = "datahub_ogg_plugin.chk";

    private Map<String, TableMapping> tableMappings;

    private boolean isCheckPointFileDisable = false;

    private String charsetName = "UTF-8";

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
        this.batchSize = batchSize;
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

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public String getCheckPointFileName() {
        return checkPointFileName;
    }

    public void setCheckPointFileName(String checkPointFileName) {
        this.checkPointFileName = checkPointFileName;
    }

    public boolean isCheckPointFileDisable() {
        return isCheckPointFileDisable;
    }

    public void setCheckPointFileDisable(boolean checkPointFileDisable) {
        isCheckPointFileDisable = checkPointFileDisable;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public Map<String, TableMapping> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(Map<String, TableMapping> tableMappings) {
        this.tableMappings = tableMappings;
    }

    public void addTableMapping(TableMapping tableMapping) {
        if (this.tableMappings == null) {
            this.tableMappings = Maps.newHashMap();
        }

        this.tableMappings.put(tableMapping.getOracleFullTableName(), tableMapping);
    }

    public TableMapping getTableMapping(String oracleFullTableName) {
        return this.tableMappings.get(oracleFullTableName);
    }


}
