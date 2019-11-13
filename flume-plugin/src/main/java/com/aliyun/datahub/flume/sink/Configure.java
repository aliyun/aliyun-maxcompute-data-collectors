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

public class Configure {

    public static final String DEFAULT_DATAHUB_END_POINT = "http://dh.odps.aliyun.com";

    public static final int DEFAULT_DATAHUB_BATCHSIZE = 1000;

    public static final int DEFAULT_DATAHUB_BATCHTIMEOUT = 5;

    public static final int DEFAULT_DATAHUB_MAX_BUFFERSIZE = 2 * 1024 * 1024;

    public static final boolean DEFAULT_ENABLE_PB = true;

    public static final int DEFAULT_RETRY_TIMES = 3;
    /**
     * Default retry interval, unit is second.
     */
    public static final int DEFAULT_RETRY_INTERVAL = 5;

    public static final boolean DEFAULT_DIRTY_DATA_CONTINUE = true;
    public static final String DEFAULT_DIRTY_DATA_FILE = "DataHub-Flume-dirty-file";


    private int retryTimes;
    private int retryInterval;

    private String accessId;
    private String accessKey;
    private String endPoint;
    private String project;
    private String topic;
    private boolean enablePb;

    private String serializerType;
    private int batchSize;
    private int maxBufferSize;
    private int batchTimeout;
    private List<String> shardIds;

    private String compressType;

    private boolean dirtyDataContinue;
    private String dirtyDataFile;

    private String[] inputColumnNames;


    public String[] getInputColumnNames() {
        return inputColumnNames;
    }


    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("retryTimes\t" + retryTimes + "\n");
        builder.append("retryInterval\t" + retryInterval + "\n");
        //builder.append("accessId\t" + accessId + "\n");
        //builder.append("accessKey\t" + accessKey + "\n");
        builder.append("endPoint\t" + endPoint + "\n");
        builder.append("project\t" + project + "\n");
        builder.append("topic\t" + topic + "\n");
        builder.append("enablePb\t" + enablePb + "\n");
        builder.append("serializerType\t" + serializerType + "\n");
        builder.append("batchSize\t" + batchSize + "\n");
        builder.append("maxBufferSize\t" + maxBufferSize + "\n");
        builder.append("batchTimeout\t" + batchTimeout + "\n");
        builder.append("shardIds\t" + getShardIdsString() + "\n");
        builder.append("compressType\t" + compressType + "\n");
        builder.append("dirtyDataContinue\t" + dirtyDataContinue + "\n");
        builder.append("dirtyDataFile\t" + dirtyDataFile + "\n");
        return builder.toString();
    }


    public void setInputColumnNames(String[] inputColumnNames) {
        this.inputColumnNames = inputColumnNames;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isEnablePb() {
        return enablePb;
    }

    public void setEnablePb(boolean enablePb) {
        this.enablePb = enablePb;
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

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(int batchTimeout) {
        this.batchTimeout = batchTimeout;
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public void setShardIds(List<String> shardIds) {
        this.shardIds = shardIds;
    }

    public String getShardIdsString() {
        if (shardIds == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < shardIds.size(); ++i) {
            builder.append(shardIds.get(i));
            if (i != shardIds.size() - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
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
}
