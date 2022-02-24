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

package com.aliyun.odps.cupid.table.v1.writer;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.TableUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class TableWriteSession {

    protected final String project;
    protected final String table;
    protected final Map<String, String> partitionSpec;
    protected final boolean overwrite;
    protected TableSchema tableSchema;
    protected Options options;

    protected TableWriteSession(String project,
                                String table,
                                TableSchema tableSchema,
                                Map<String, String> partitionSpec,
                                boolean overwrite) {
        this.project = project;
        this.table = table;
        this.tableSchema = tableSchema == null? getTableSchema() : tableSchema;
        this.partitionSpec = TableUtils.getOrderedPartitionSpec(this.tableSchema, partitionSpec);
        this.overwrite = overwrite;
    }

    protected TableWriteSession(String project,
                                String table,
                                Options options,
                                TableSchema tableSchema,
                                Map<String, String> partitionSpec,
                                boolean overwrite) {
        this.project = project;
        this.table = table;
        this.options = options;
        this.tableSchema = tableSchema == null? getTableSchema() : tableSchema;
        this.partitionSpec = TableUtils.getOrderedPartitionSpec(this.tableSchema, partitionSpec);
        this.overwrite = overwrite;
    }

    public abstract TableSchema getTableSchema();

    public abstract WriteSessionInfo getOrCreateSessionInfo() throws IOException;

    public abstract void commitTable() throws IOException;

    public abstract void commitTableWithPartitions(List<Map<String, String>> partitionSpecs) throws IOException;

    public abstract void commitClusteredTable(List<BucketFileInfo> partitionSpecs) throws IOException;

    public void commitTableWithMessage(List<WriterCommitMessage> commitMessages) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract void cleanup() throws IOException;
}
