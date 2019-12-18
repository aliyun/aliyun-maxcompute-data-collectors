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


import java.util.Map;

/**
 *
 * @author lyf0429
 * @date 16/5/25
 */
public class DirtyRecordInfo {
    private String oracleTable;
    private String topicName;
    private String shardId;
    private String errorMessage;
    private String errorTime;
    private Map<String, String> record;

    public String getOracleTable() {
        return oracleTable;
    }

    public void setOracleTable(String oracleTable) {
        this.oracleTable = oracleTable;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Map<String, String> getRecord() {
        return record;
    }

    public void setRecord(Map<String, String> record) {
        this.record = record;
    }

    public String getErrorTime() {
        return errorTime;
    }

    public void setErrorTime(String errorTime) {
        this.errorTime = errorTime;
    }
}
