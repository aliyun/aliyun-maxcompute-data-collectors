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
import com.aliyun.odps.cupid.table.v1.util.Builder;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.ProviderRegistry;
import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.*;

public final class TableWriteSessionBuilder extends Builder {

    private String provider;
    private String project;
    private String table;
    private boolean overwrite;
    private TableSchema tableSchema;
    private Map<String, String> partitionSpec;
    private WriteSessionInfo writeSessionInfo;
    private Options options;

    public TableWriteSessionBuilder(String provider, String project, String table) {
        Validator.checkString(provider, "provider");
        Validator.checkString(project, "project");
        Validator.checkString(table, "table");
        this.provider = provider;
        this.project = project;
        this.table = table;
    }

    public static WriteCapabilities getProviderCapabilities(String provider)
            throws ClassNotFoundException {
        Validator.checkString(provider, "provider");
        return ProviderRegistry.lookup(provider).getWriteCapabilities();
    }

    public TableWriteSessionBuilder options(Options options) {
        Validator.checkNotNull(options, "options");
        this.options = options;
        return this;
    }

    public TableWriteSessionBuilder tableSchema(TableSchema tableSchema) {
        Validator.checkNotNull(tableSchema, "tableSchema");
        this.tableSchema = tableSchema;
        return this;
    }

    public TableWriteSessionBuilder partitionSpec(Map<String, String> partitionSpec) {
        Validator.checkNotNull(partitionSpec, "partitionSpec");
        this.partitionSpec = partitionSpec;
        return this;
    }

    public TableWriteSessionBuilder overwrite(boolean isOverwrite) {
        this.overwrite = isOverwrite;
        return this;
    }

    public TableWriteSessionBuilder writeSessionInfo(WriteSessionInfo writeSessionInfo) {
        Validator.checkNotNull(writeSessionInfo, "writeSessionInfo");
        this.writeSessionInfo = writeSessionInfo;
        return this;
    }

    public TableWriteSession build() throws ClassNotFoundException {
        if (writeSessionInfo == null){
            sanitize();
            return ProviderRegistry.lookup(provider).createWriteSession(
                    project, table, tableSchema, partitionSpec, overwrite, options);
        } else {
            markBuilt();
            return ProviderRegistry.lookup(provider).getWriteSession(writeSessionInfo);
        }
    }

    private void sanitize() {
        markBuilt();

        if (partitionSpec == null) {
            partitionSpec = Collections.emptyMap();
        } else {
            partitionSpec = Collections.unmodifiableMap(new HashMap<>(partitionSpec));
        }
    }
}
