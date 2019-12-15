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

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public String getDestOld() {
        return destOld;
    }

    public void setDestOld(String destOld) {
        this.destOld = destOld;
    }

    public boolean isShardColumn() {
        return isShardColumn;
    }

    public void setIsShardColumn(boolean isShardColumn) {
        this.isShardColumn = isShardColumn;
    }

    public boolean isDateFormat() {
        return isDateFormat;
    }

    public void setIsDateFormat(boolean isDateFormat) {
        this.isDateFormat = isDateFormat;
    }

    public boolean isKeyColumn() {
        return isKeyColumn;
    }

    public void setIsKeyColumn(boolean keyColumn) {
        isKeyColumn = keyColumn;
    }

    public SimpleDateFormat getSimpleDateFormat() {
        return simpleDateFormat;
    }

    public void setSimpleDateFormat(SimpleDateFormat simpleDateFormat) {
        this.simpleDateFormat = simpleDateFormat;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
        simpleDateFormat = new SimpleDateFormat(dateFormat);
    }
}
