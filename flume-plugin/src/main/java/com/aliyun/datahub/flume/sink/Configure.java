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


public class Configure {

    public static final String DEFAULT_DATAHUB_END_POINT = "http://dh.odps.aliyun.com";
    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final int DEFAULT_DATAHUB_BATCHSIZE = 100;

    public static final int DEFAULT_RETRY_TIMES = -1;
    /**
     * Default retry interval, unit is second.
     */
    public static final int DEFAULT_RETRY_INTERVAL = 5;

    private int retryTimes;
    private int retryInterval;
    private String accessId;
    private String accessKey;
    private String endPoint;
    private String project;
    private String topic;
    private String dateFormat;
    private String serializerType;
    private int batchSize;
    private String shardId;
    private String[] inputColumnNames;

    private String[] shardColumnNames;
    private String[] dateformatColumnNames;

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
}
