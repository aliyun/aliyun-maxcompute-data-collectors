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
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;

import java.util.List;
import java.util.Map;

public class TunnelWriteSessionInfo extends WriteSessionInfo {

    private final String uploadId;
    private final boolean isStream;
    private final boolean isDynamicPartition;

    TunnelWriteSessionInfo(String project,
                           String table,
                           List<Attribute> dataColumns,
                           List<Attribute> partitionColumns,
                           Map<String, String> partitionSpec,
                           String uploadId,
                           boolean overwrite,
                           boolean isDynamicPartition,
                           boolean isStream,
                           Options options) {
        super(project, table, dataColumns, partitionColumns, partitionSpec, overwrite, options);
        this.uploadId = uploadId;
        this.isDynamicPartition = isDynamicPartition;
        this.isStream = isStream;
    }

    @Override
    public String getProvider() {
        return "tunnel";
    }

    public String getUploadId() {
        return uploadId;
    }

    public boolean isStream() { return isStream; }

    public boolean isDynamicPartition() {
        return isDynamicPartition;
    }
}
