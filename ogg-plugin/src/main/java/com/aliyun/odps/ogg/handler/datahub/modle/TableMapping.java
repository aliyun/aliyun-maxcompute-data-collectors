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

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.wrapper.Topic;

import java.util.Map;

/**
 * Created by lyf0429 on 16/5/15.
 */
public class TableMapping {
    private String oracleSchema;

    private String oracleTableName;

    private String oracleFullTableName;

    private Topic topic;

    private Field ctypeField;

    private Field ctimeField;

    private Field cidField;

    private Map<String, ColumnMapping> columnMappings;

    private Map<String, Field> constFieldMappings;

    private Map<String, String> constColumnMappings;

    private String shardId;

    private boolean isShardHash = false;

    public String getOracleFullTableName() {
        return oracleFullTableName;
    }

    public void setOracleFullTableName(String oracleFullTableName) {
        this.oracleFullTableName = oracleFullTableName;
    }

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
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

    public Field getCtimeField() {
        return ctimeField;
    }

    public void setCtimeField(Field ctimeField) {
        this.ctimeField = ctimeField;
    }

    public Field getCtypeField() {
        return ctypeField;
    }

    public void setCtypeField(Field ctypeField) {
        this.ctypeField = ctypeField;
    }

    public Field getCidField() {
        return cidField;
    }

    public void setCidField(Field cidField) {
        this.cidField = cidField;
    }

    public Map<String, ColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public void setColumnMappings(Map<String, ColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public boolean isShardHash() {
        return isShardHash;
    }

    public void setIsShardHash(boolean isShardHash) {
        this.isShardHash = isShardHash;
    }


    public Map<String, Field> getConstFieldMappings() {
        return constFieldMappings;
    }

    public void setConstFieldMappings(Map<String, Field> constFieldMappings) {
        this.constFieldMappings = constFieldMappings;
    }

    public Map<String, String> getConstColumnMappings() {
        return constColumnMappings;
    }

    public void setConstColumnMappings(Map<String, String> constColumnMappings) {
        this.constColumnMappings = constColumnMappings;
    }

}
