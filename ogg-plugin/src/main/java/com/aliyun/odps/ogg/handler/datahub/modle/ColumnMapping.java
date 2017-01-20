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
import org.codehaus.jackson.annotate.JsonIgnore;

import java.text.SimpleDateFormat;

/**
 * Created by lyf0429 on 16/5/20.
 */
public class ColumnMapping {
    private String oracleColumnName;
    private Field field;
    private Field oldFiled;
    private boolean isShardColumn = false;
    @JsonIgnore private SimpleDateFormat simpleDateFormat;
    private boolean isDateFormat = true;

    public String getOracleColumnName() {
        return oracleColumnName;
    }

    public void setOracleColumnName(String oracleColumnName) {
        this.oracleColumnName = oracleColumnName;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }

    public Field getOldFiled() {
        return oldFiled;
    }

    public void setOldFiled(Field oldFiled) {
        this.oldFiled = oldFiled;
    }

    public boolean isShardColumn() {
        return isShardColumn;
    }

    public void setIsShardColumn(boolean isShardColumn) {
        this.isShardColumn = isShardColumn;
    }

    public SimpleDateFormat getSimpleDateFormat() {
        return simpleDateFormat;
    }

    public void setSimpleDateFormat(SimpleDateFormat simpleDateFormat) {
        this.simpleDateFormat = simpleDateFormat;
    }

    public boolean isDateFormat() {
        return isDateFormat;
    }

    public void setIsDateFormat(boolean isDateFormat) {
        this.isDateFormat = isDateFormat;
    }
}
