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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.text.SimpleDateFormat;

/**
 * Created by lyf0429 on 16/5/20.
 */
public class ColumnMapping {
    private String src;
    private String dest;
    private String destOld;

    private boolean isShardColumn = false;
    private boolean isKeyColumn = false;

    private boolean isDateFormat = true;

    @JsonIgnore private SimpleDateFormat simpleDateFormat;
    private String dateFormat;

    public String getSrc() {
        return src;
    }

    public ColumnMapping setSrc(String src) {
        this.src = src;
        return this;
    }

    public String getDest() {
        return dest;
    }

    public ColumnMapping setDest(String dest) {
        this.dest = dest;
        return this;
    }

    public String getDestOld() {
        return destOld;
    }

    public ColumnMapping setDestOld(String destOld) {
        this.destOld = destOld;
        return this;
    }

    public boolean isShardColumn() {
        return isShardColumn;
    }

    public ColumnMapping setIsShardColumn(boolean isShardColumn) {
        this.isShardColumn = isShardColumn;
        return this;
    }

    public boolean isDateFormat() {
        return isDateFormat;
    }

    public ColumnMapping setIsDateFormat(boolean isDateFormat) {
        this.isDateFormat = isDateFormat;
        return this;
    }

    public boolean isKeyColumn() {
        return isKeyColumn;
    }

    public ColumnMapping setIsKeyColumn(boolean keyColumn) {
        isKeyColumn = keyColumn;
        return this;
    }

    public SimpleDateFormat getSimpleDateFormat() {
        return simpleDateFormat;
    }

    public ColumnMapping setSimpleDateFormat(SimpleDateFormat simpleDateFormat) {
        this.simpleDateFormat = simpleDateFormat;
        return this;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public ColumnMapping setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        simpleDateFormat = new SimpleDateFormat(dateFormat);
        return this;
    }
}
