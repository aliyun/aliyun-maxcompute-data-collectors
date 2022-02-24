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

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.util.Options;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class WriteSessionInfo implements Serializable {

    private final String project;
    private final String table;
    private final List<Attribute> dataColumns;
    private final List<Attribute> partitionColumns;
    private final Map<String, String> partitionSpec;
    private final boolean overwrite;
    private final Options options;

    protected WriteSessionInfo(String project,
                               String table,
                               List<Attribute> dataColumns,
                               List<Attribute> partitionColumns,
                               Map<String, String> partitionSpec) {
        this(project, table, dataColumns, partitionColumns, partitionSpec, false);
    }

    protected WriteSessionInfo(String project,
                               String table,
                               List<Attribute> dataColumns,
                               List<Attribute> partitionColumns,
                               Map<String, String> partitionSpec,
                               boolean overwrite) {
        this(project, table, dataColumns, partitionColumns, partitionSpec, false, null);
    }

    protected WriteSessionInfo(String project,
                               String table,
                               List<Attribute> dataColumns,
                               List<Attribute> partitionColumns,
                               Map<String, String> partitionSpec,
                               boolean overwrite,
                               Options options) {
        this.project = project;
        this.table = table;
        this.dataColumns = dataColumns;
        this.partitionColumns = partitionColumns;
        this.partitionSpec = partitionSpec;
        this.overwrite = overwrite;
        this.options = options == null ? new Options.OptionsBuilder().build() : options;
    }

    public abstract String getProvider();

    public String getProject() {
        return project;
    }

    public String getTable() {
        return table;
    }

    public List<Attribute> getDataColumns() {
        return dataColumns;
    }

    public List<Attribute> getPartitionColumns() {
        return partitionColumns;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    public boolean supportColData() {
        return false;
    }

    public boolean supportArrow() {
        return false;
    }

    public boolean supportDynamicPartitionCol() {
        return false;
    }

    public boolean isOverwrite() { return overwrite; }

    public Options getOptions() {
        return options;
    }

}
