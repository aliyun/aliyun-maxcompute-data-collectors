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

    private String projectName;

    private String topicName;

    private String rowIdColumn;

    private String cTypeColumn;
    private String cTimeColumn;
    private String cIdColumn;
    private Map<String, String> constColumnMappings;

    private RecordSchema recordSchema;

    @JsonIgnore
    private Map<String, ColumnMapping> columnMappings;

    @JsonIgnore
    private boolean shardHash = false;

    public String getOracleFullTableName() {
        return oracleFullTableName;
    }

    public TableMapping setOracleFullTableName(String oracleFullTableName) {
        this.oracleFullTableName = oracleFullTableName;
        return this;
    }

    public String getProjectName() {
        return projectName;
    }

    public TableMapping setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getRowIdColumn() {
        return rowIdColumn;
    }

    public TableMapping setRowIdColumn(String rowIdColumn) {
        this.rowIdColumn = rowIdColumn;
        return this;
    }

    public TableMapping setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public String getcTypeColumn() {
        return cTypeColumn;
    }

    public TableMapping setcTypeColumn(String cTypeColumn) {
        this.cTypeColumn = cTypeColumn;
        return this;
    }

    public String getcTimeColumn() {
        return cTimeColumn;
    }

    public TableMapping setcTimeColumn(String cTimeColumn) {
        this.cTimeColumn = cTimeColumn;
        return this;
    }

    public String getcIdColumn() {
        return cIdColumn;
    }

    public TableMapping setcIdColumn(String cIdColumn) {
        this.cIdColumn = cIdColumn;
        return this;
    }

    public RecordSchema getRecordSchema() {
        return recordSchema;
    }

    public TableMapping setRecordSchema(RecordSchema recordSchema) {
        this.recordSchema = recordSchema;
        return this;
    }

    public String getOracleSchema() {
        return oracleSchema;
    }

    public TableMapping setOracleSchema(String oracleSchema) {
        this.oracleSchema = oracleSchema;
        return this;
    }

    public String getOracleTableName() {
        return oracleTableName;
    }

    public TableMapping setOracleTableName(String oracleTableName) {
        this.oracleTableName = oracleTableName;
        return this;
    }

    public Map<String, ColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public TableMapping setColumnMappings(Map<String, ColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
        return this;
    }

    public boolean isShardHash() {
        return shardHash;
    }

    public TableMapping setShardHash(boolean shardHash) {
        this.shardHash = shardHash;
        return this;
    }

    public Map<String, String> getConstColumnMappings() {
        return constColumnMappings;
    }

    public TableMapping setConstColumnMappings(Map<String, String> constColumnMappings) {
        this.constColumnMappings = constColumnMappings;
        return this;
    }
}
