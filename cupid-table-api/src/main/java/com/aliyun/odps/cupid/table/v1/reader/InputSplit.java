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

package com.aliyun.odps.cupid.table.v1.reader;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.filter.FilterExpression;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class InputSplit implements Serializable {

    private final String project;
    private final String table;

    private final List<Attribute> dataColumns;
    private final List<Attribute> partitionColumns;
    private final List<Attribute> readDataColumns;
    private final Map<String, String> partitionSpec;

    protected InputSplit(String project,
                         String table,
                         List<Attribute> dataColumns,
                         List<Attribute> partitionColumns,
                         List<Attribute> readDataColumns,
                         Map<String, String> partitionSpec) {
        this.project = project;
        this.table = table;
        this.dataColumns = dataColumns;
        this.partitionColumns = partitionColumns;
        this.readDataColumns = readDataColumns;
        this.partitionSpec = partitionSpec;
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

    public List<Attribute> getReadDataColumns() {
        return readDataColumns;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    protected int getBucketId() {
        throw new UnsupportedOperationException();
    }

    protected List<FilterExpression> getFilterExpressions() {
        throw new UnsupportedOperationException();
    }

    protected List<Attribute> getFunctionCalls() {
        throw new UnsupportedOperationException();
    }

    public boolean supportColData() {
        return false;
    }

    public boolean supportArrow() {
        return false;
    }
}
