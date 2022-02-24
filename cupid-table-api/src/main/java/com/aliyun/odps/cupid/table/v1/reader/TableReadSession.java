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

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.filter.FilterExpression;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.TableUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class TableReadSession {

    protected final String project;
    protected final String table;
    protected Options options;

    protected TableSchema tableSchema;
    protected final RequiredSchema readDataColumns;
    protected final List<Map<String, String>> partitionSpecs;

    protected int splitSizeInMB;
    protected int splitParallelism;
    protected boolean splitAtBucketLevel;

    protected TableReadSession(String project,
                               String table,
                               TableSchema tableSchema,
                               RequiredSchema readDataColumns,
                               List<Map<String, String>> partitionSpecs) {
        this.project = project;
        this.table = table;
        this.tableSchema = tableSchema == null ? getTableSchema() : tableSchema;
        this.readDataColumns = readDataColumns;
        this.partitionSpecs = partitionSpecs.stream()
                .map(partitionSpec -> TableUtils.getOrderedPartitionSpec(this.tableSchema, partitionSpec))
                .collect(Collectors.toList());
    }

    protected TableReadSession(String project,
                               String table,
                               Options options,
                               TableSchema tableSchema,
                               RequiredSchema readDataColumns,
                               List<Map<String, String>> partitionSpecs) {
        this.project = project;
        this.table = table;
        this.options = options;
        this.tableSchema = tableSchema == null ? getTableSchema() : tableSchema;
        this.readDataColumns = readDataColumns;
        this.partitionSpecs = partitionSpecs.stream()
                .map(partitionSpec -> TableUtils.getOrderedPartitionSpec(this.tableSchema, partitionSpec))
                .collect(Collectors.toList());
    }

    public TableSchema getTableSchema() {
        return this.tableSchema;
    }

    public InputSplit[] getOrCreateInputSplits() throws IOException {
        throw new UnsupportedOperationException();
    }

    protected void setBucketFilter(List<Integer> bucketFilter) {
        throw new UnsupportedOperationException();
    }

    protected void setPartitionBucketFilters(List<List<Integer>> partitionBucketFilters) {
        throw new UnsupportedOperationException();
    }

    protected void setSplitBySize(int splitSizeInMB) {
        this.splitSizeInMB = splitSizeInMB;
    }

    protected void setSplitByParallelism(int splitParallelism) {
        this.splitParallelism = splitParallelism;
    }

    protected void setSplitAtBucketLevel(boolean splitAtBucketLevel) {
        this.splitAtBucketLevel = splitAtBucketLevel;
    }

    protected void setFilterExpressions(List<FilterExpression> filterExpressions) {
        throw new UnsupportedOperationException();
    }

    protected void setFunctionCalls(List<Attribute> functionCalls) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public abstract InputSplit[] getOrCreateInputSplits(int splitSizeInMB) throws IOException;
}
