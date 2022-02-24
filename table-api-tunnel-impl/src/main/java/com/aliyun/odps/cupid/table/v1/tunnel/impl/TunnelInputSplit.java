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

package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.util.Options;

import java.util.List;
import java.util.Map;

public class TunnelInputSplit extends InputSplit {

    private String downloadId;
    private long startIndex;
    private long numRecord;
    private Options options;

    protected TunnelInputSplit(String project,
                               String table,
                               List<Attribute> dataColumns,
                               List<Attribute> partitionColumns,
                               List<Attribute> readDataColumns,
                               Map<String, String> partitionSpec,
                               String downloadId,
                               long startIndex,
                               long numRecord,
                               Options options) {
        super(project, table, dataColumns, partitionColumns, readDataColumns, partitionSpec);
        this.downloadId = downloadId;
        this.startIndex = startIndex;
        this.numRecord = numRecord;
        this.options = options;
    }

    @Override
    public String getProvider() {
        return "tunnel";
    }

    public String getDownloadId() {
        return downloadId;
    }

    public long getStartIndex() {
        return startIndex;
    }

    public long getNumRecord() {
        return numRecord;
    }

    public Options getOptions() {
        return options;
    }

    public void setDownloadId(String downloadId) {
        this.downloadId = downloadId;
    }

    public void setStartIndex(long startIndex) {
        this.startIndex = startIndex;
    }

    public void setNumRecord(long numRecord) {
        this.numRecord = numRecord;
    }

    public void setOptions(Options options) {
        this.options = options;
    }
}
