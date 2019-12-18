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

import com.aliyun.datahub.client.model.RecordSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by lyf0429 on 16/5/15.
 */
public class TableMapping {
    private String oracleSchema;

    private String oracleTableName;

    private String oracleFullTableName;

    @Deprecated
    private String accessId;

    @Deprecated
    private String accessKey;

    private String projectName;

    private String topicName;

    private String rowIdColumn;

    private String cTypeColumn;
    private String cTimeColumn;
    private String cIdColumn;
    private Map<String, String> constColumnMappings;

    private RecordSchema recordSchema;

    private boolean setShardId = false;

    private List<String> shardIds;

    private Map<String, ColumnMapping> columnMappings;

    @JsonIgnore
    private boolean shardHash = false;

    public String getOracleFullTableName() {
        return oracleFullTableName;
    }

    public void setOracleFullTableName(String oracleFullTableName) {
        this.oracleFullTableName = oracleFullTableName;
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

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getRowIdColumn() {
        return rowIdColumn;
    }

    public void setRowIdColumn(String rowIdColumn) {
        this.rowIdColumn = rowIdColumn;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getcTypeColumn() {
        return cTypeColumn;
    }

    public void setcTypeColumn(String cTypeColumn) {
        this.cTypeColumn = cTypeColumn;
    }

    public String getcTimeColumn() {
        return cTimeColumn;
    }

    public void setcTimeColumn(String cTimeColumn) {
        this.cTimeColumn = cTimeColumn;
    }

    public String getcIdColumn() {
        return cIdColumn;
    }

    public void setcIdColumn(String cIdColumn) {
        this.cIdColumn = cIdColumn;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public void setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
    }

    public boolean isSetShardId() {
        return setShardId;
    }

    public void setSetShardId(boolean setShardId) {
        this.setShardId = setShardId;
    }

    public List<String> getShardIds() {
        return shardIds;
    }

    public void setShardIds(List<String> shardIds) {
        this.shardIds = shardIds;
    }

    @JsonIgnore
    public String getShardId() {
        int index = new Random().nextInt(shardIds.size());
        return shardIds.get(index);
    }

    public String getOracleSchema() {
        return oracleSchema;
    }

    public void setOracleSchema(String oracleSchema) {
        this.oracleSchema = oracleSchema;
    }

    public String getOracleTableName() {
        return oracleTableName;
    }

    public void setOracleTableName(String oracleTableName) {
        this.oracleTableName = oracleTableName;
    }

    public Map<String, ColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public void setColumnMappings(Map<String, ColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public boolean isShardHash() {
        return shardHash;
    }

    public void setShardHash(boolean shardHash) {
        this.shardHash = shardHash;
    }

    public Map<String, String> getConstColumnMappings() {
        return constColumnMappings;
    }

    public void setConstColumnMappings(Map<String, String> constColumnMappings) {
        this.constColumnMappings = constColumnMappings;
    }

}
