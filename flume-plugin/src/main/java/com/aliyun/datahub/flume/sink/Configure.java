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

package com.aliyun.datahub.flume.sink;


import java.util.List;
import java.util.TimeZone;

public class Configure {

    public static final String DEFAULT_DATAHUB_END_POINT = "http://dh.odps.aliyun.com";

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final int DEFAULT_DATAHUB_BATCHSIZE = 100;

    public static final int DEFAULT_RETRY_TIMES = -1;
    /**
     * Default retry interval, unit is second.
     */
    public static final int DEFAULT_RETRY_INTERVAL = 5;

    public static final boolean DEFAULT_NEED_ROUNDING = false;

    public static final String DEFAULT_MAXCOMPUTE_PARTITION_COLUMNS = "";
    public static final String DEFAULT_MAXCOMPUTE_PARTITION_VALUES = "";

    private int retryTimes;
    private int retryInterval;
    // datahub related config
    private String datahubAccessId;
    private String datahubAccessKey;
    private String datahubEndPoint;
    private String datahubProject;
    private String datahubTopic;
    private int datahubShardCount;
    private int datahubLifeCycle;
    private String datahubSchema;

    private String dateFormat;
    private String serializerType;
    private int batchSize;
    private String shardId;
    private String[] inputColumnNames;

    private String[] shardColumnNames;
    private String[] dateformatColumnNames;

    private boolean useLocalTime;
    private TimeZone timeZone;
    private boolean needRounding;
    private int roundUnit;
    private int roundValue;

    private boolean isBlankValueAsNull;

    private List<String> maxcomputePartitionCols;
    private List<String> maxcomputePartitionVals;

    public String[] getShardColumnNames() {
        return shardColumnNames;
    }

    public void setShardColumnNames(String[] shardColumnNames) {
        this.shardColumnNames = shardColumnNames;
    }

    public String[] getDateformatColumnNames() {
        return dateformatColumnNames;
    }

    public void setDateformatColumnNames(String[] dateformatColumnNames) {
        this.dateformatColumnNames = dateformatColumnNames;
    }

    public String[] getInputColumnNames() {
        return inputColumnNames;
    }

    public void setInputColumnNames(String[] inputColumnNames) {
        this.inputColumnNames = inputColumnNames;
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

    public String getDatahubEndPoint() {
        return datahubEndPoint;
    }

    public void setDatahubEndPoint(String datahubEndPoint) {
        this.datahubEndPoint = datahubEndPoint;
    }

    public String getDatahubProject() {
        return datahubProject;
    }

    public void setDatahubProject(String datahubProject) {
        this.datahubProject = datahubProject;
    }

    public String getDatahubTopic() {
        return datahubTopic;
    }

    public void setDatahubTopic(String datahubTopic) {
        this.datahubTopic = datahubTopic;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getSerializerType() {
        return serializerType;
    }

    public void setSerializerType(String serializerType) {
        this.serializerType = serializerType;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getRetryTimes() {
        return retryTimes;
    }


    public boolean isUseLocalTime() {
        return useLocalTime;
    }

    public void setUseLocalTime(boolean useLocalTime) {
        this.useLocalTime = useLocalTime;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isNeedRounding() {
        return needRounding;
    }

    public void setNeedRounding(boolean needRounding) {
        this.needRounding = needRounding;
    }

    public int getRoundUnit() {
        return roundUnit;
    }

    public void setRoundUnit(int roundUnit) {
        this.roundUnit = roundUnit;
    }

    public int getRoundValue() {
        return roundValue;
    }

    public void setRoundValue(int roundValue) {
        this.roundValue = roundValue;
    }


    public List<String> getMaxcomputePartitionCols() {
        return maxcomputePartitionCols;
    }

    public void setMaxcomputePartitionCols(List<String> maxcomputePartitionCols) {
        this.maxcomputePartitionCols = maxcomputePartitionCols;
    }

    public List<String> getMaxcomputePartitionVals() {
        return maxcomputePartitionVals;
    }

    public void setMaxcomputePartitionVals(List<String> maxcomputePartitionVals) {
        this.maxcomputePartitionVals = maxcomputePartitionVals;
    }

    public boolean isBlankValueAsNull() {
        return isBlankValueAsNull;
    }

    public void setBlankValueAsNull(boolean blankValueAsNull) {
        isBlankValueAsNull = blankValueAsNull;
    }

}
