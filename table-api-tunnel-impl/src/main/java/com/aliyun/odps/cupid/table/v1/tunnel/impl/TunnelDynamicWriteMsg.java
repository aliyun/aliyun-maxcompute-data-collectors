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

import com.aliyun.odps.cupid.table.v1.writer.WriterCommitMessage;
import java.util.Map;
import java.util.Objects;

public class TunnelDynamicWriteMsg implements WriterCommitMessage {

    private final String project;
    private final String table;
    private final Map<String, String> partitionSpec;
    private final String uploadId;

    public TunnelDynamicWriteMsg(String project,
                          String table,
                          Map<String, String> partitionSpec,
                          String uploadId) {
        this.project = project;
        this.table = table;
        this.partitionSpec = partitionSpec;
        this.uploadId = uploadId;
    }

    public String getProject() {
        return project;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    public String getUploadId() {
        return uploadId;
    }

    @Override
    public String toString() {
        return "TunnelDynamicWriteMsg{"
                + "project="
                + project
                + ", table="
                + table
                + ", partitionSpec="
                + partitionSpec
                + ", uploadId="
                + uploadId
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TunnelDynamicWriteMsg that = (TunnelDynamicWriteMsg) o;
        return project.equals(that.project)
                && table.equals(that.table)
                && partitionSpec.equals(that.partitionSpec)
                && uploadId.equals(that.uploadId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                project,
                table,
                partitionSpec,
                uploadId);
    }
}
