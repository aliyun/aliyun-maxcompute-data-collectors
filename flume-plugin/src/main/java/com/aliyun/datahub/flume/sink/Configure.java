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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Configure {
    private static final Logger logger = LoggerFactory.getLogger(Configure.class);

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

    public static final boolean DEFAULT_AUTO_COMMIT = true;
    public static final int DEFAULT_OFFSET_COMMIT_INTERVAL = 30;
    public static final int DEFAULT_SESSION_TIMEOUT = 60;


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

    private String subId;
    private long startTimestamp;
    private boolean autoCommit;
    private int offsetCommitInterval;
    private int sessionTimeout;

    private String[] inputColumnNames;


    public String[] getInputColumnNames() {
        return inputColumnNames;
    }

    public String sinktoString() {
        StringBuilder builder = new StringBuilder();
        builder.append("endPoint\t" + endPoint + "\n");
        builder.append("accessId\t" + accessId + "\n");
        builder.append("accessKey\t" + accessKey + "\n");
        builder.append("project\t" + project + "\n");
        builder.append("topic\t" + topic + "\n");
        builder.append("shardIds\t" + getShardIdsString() + "\n");
        builder.append("enablePb\t" + enablePb + "\n");
        builder.append("compressType\t" + compressType + "\n");
        builder.append("batchSize\t" + batchSize + "\n");
        builder.append("maxBufferSize\t" + maxBufferSize + "\n");
        builder.append("batchTimeout\t" + batchTimeout + "\n");
        builder.append("retryTimes\t" + retryTimes + "\n");
        builder.append("retryInterval\t" + retryInterval + "\n");
        builder.append("dirtyDataContinue\t" + dirtyDataContinue + "\n");
        builder.append("dirtyDataFile\t" + dirtyDataFile + "\n");
        builder.append("serializer\t" + serializerType + "\n");
        return builder.toString();
    }

    public String sourcetoString() {
        StringBuilder builder = new StringBuilder();
        builder.append("endPoint\t" + endPoint + "\n");
        builder.append("accessId\t" + accessId + "\n");
        builder.append("accessKey\t" + accessKey + "\n");
        builder.append("project\t" + project + "\n");
        builder.append("topic\t" + topic + "\n");
        builder.append("subId\t" + subId + "\n");
        builder.append("startTimestamp\t" + startTimestamp + "\n");
        builder.append("shardIds\t" + getShardIdsString() + "\n");
        builder.append("enablePb\t" + enablePb + "\n");
        builder.append("compressType\t" + compressType + "\n");
        builder.append("batchSize\t" + batchSize + "\n");
        builder.append("batchTimeout\t" + batchTimeout + "\n");
        builder.append("retryTimes\t" + retryTimes + "\n");
        builder.append("autoCommit\t" + autoCommit + "\n");
        builder.append("offsetCommitInterval\t" + offsetCommitInterval + "\n");
        builder.append("sessionTimeout\t" + sessionTimeout + "\n");
        builder.append("serializer\t" + serializerType + "\n");
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

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(String startTime) {
        //startTime = "2019-07-01 09:00:00";
        if (startTime == null) {
            this.startTimestamp = -1;
            return;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = simpleDateFormat.parse(startTime);
        } catch (ParseException e) {
            logger.error("startTime {} parse failed. ", startTime, e);
            throw new IllegalArgumentException("startTime " + startTime + " parse failed. ", e);
        }
        this.startTimestamp = date.getTime();
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
        if (batchSize <= 0) {
            logger.warn("BatchSize must be positive number. Defaulting to {}",
                    Configure.DEFAULT_DATAHUB_BATCHSIZE);
            batchSize = Configure.DEFAULT_DATAHUB_BATCHSIZE;
        }
        this.batchSize = batchSize;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        if (maxBufferSize <= 0) {
            logger.warn("MaxBufferSize must be positive number. Defaulting to {}",
                    Configure.DEFAULT_DATAHUB_MAX_BUFFERSIZE);
            maxBufferSize = Configure.DEFAULT_DATAHUB_BATCHSIZE;
        }
        this.maxBufferSize = maxBufferSize;
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(int batchTimeout) {
        if (batchTimeout <= 0) {
            logger.warn("BatchTimeout must be positive number. Defaulting to {}",
                    Configure.DEFAULT_DATAHUB_BATCHTIMEOUT);
        }
        this.batchTimeout = batchTimeout;
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public void setShardIds(List<String> shardIds) {
        this.shardIds = shardIds;
    }

    public String getShardIdsString() {
        if (shardIds == null || shardIds.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < shardIds.size(); ++i) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(shardIds.get(i));
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
        if (retryInterval <= 0) {
            logger.warn("RetryInterval must be positive number. Defaulting to {}",
                    Configure.DEFAULT_RETRY_INTERVAL);
        }
        this.retryInterval = retryInterval;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryTimes(int retryTimes) {
        if (retryTimes <= 0) {
            logger.warn("RetryInterval must be positive number. Defaulting to {}",
                    Configure.DEFAULT_RETRY_TIMES);
        }
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

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getOffsetCommitInterval() {
        return offsetCommitInterval;
    }

    public void setOffsetCommitInterval(int offsetCommitInterval) {
        if (offsetCommitInterval <= 0) {
            logger.warn("OffsetCommitInterval must be positive number. Defaulting to {}",
                    Configure.DEFAULT_OFFSET_COMMIT_INTERVAL);
        }
        this.offsetCommitInterval = offsetCommitInterval;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        if (sessionTimeout <= 0) {
            logger.warn("SessionTimeout must be positive number. Defaulting to {}",
                    Configure.DEFAULT_SESSION_TIMEOUT);
        }
        this.sessionTimeout = sessionTimeout;
    }
}
