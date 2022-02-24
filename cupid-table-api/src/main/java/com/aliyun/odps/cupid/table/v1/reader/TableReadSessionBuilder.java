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
import com.aliyun.odps.cupid.table.v1.util.*;
import com.aliyun.odps.cupid.table.v1.reader.filter.FilterExpression;
import com.aliyun.odps.cupid.table.v1.reader.function.FunctionCall;
import com.aliyun.odps.cupid.table.v1.util.ProviderRegistry;
import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.*;

public final class TableReadSessionBuilder extends Builder {

    private String provider;
    private String project;
    private String table;
    private TableSchema tableSchema;
    private RequiredSchema readDataColumns;
    private List<PartitionSpecWithBucketFilter> readPartitions;
    private List<Integer> bucketFilter;
    private boolean splitAtBucketLevel;
    private int splitSizeInMB;
    private int splitParallelism;
    private Options options;
    private List<FilterExpression> filterExpressions;
    private List<FunctionCall> functionCalls;

    private List<Map<String, String>> partitionSpecs;
    private List<List<Integer>> partitionBucketFilters;

    public static ReadCapabilities getProviderCapabilities(String provider)
            throws ClassNotFoundException {
        Validator.checkString(provider, "provider");
        return ProviderRegistry.lookup(provider).getReadCapabilities();
    }

    public TableReadSessionBuilder(String provider, String project, String table) {
        Validator.checkString(provider, "provider");
        Validator.checkString(project, "project");
        Validator.checkString(table, "table");
        this.provider = provider;
        this.project = project;
        this.table = table;
    }

    public TableReadSessionBuilder options(Options options) {
        Validator.checkNotNull(options, "options");
        this.options = options;
        return this;
    }

    public TableReadSessionBuilder tableSchema(TableSchema tableSchema) {
        Validator.checkNotNull(tableSchema, "tableSchema");
        this.tableSchema = tableSchema;
        return this;
    }

    public TableReadSessionBuilder readDataColumns(RequiredSchema readDataColumns) {
        Validator.checkNotNull(readDataColumns, "readDataColumns");
        this.readDataColumns = readDataColumns;
        return this;
    }

    public TableReadSessionBuilder readPartitions(List<PartitionSpecWithBucketFilter> readPartitions) {
        Validator.checkNotNull(readPartitions, "readPartitions");
        this.readPartitions = readPartitions;
        return this;
    }

    public TableReadSessionBuilder splitBySize(int splitSizeInMB) {
        Validator.checkInteger(splitSizeInMB, 1,"splitSizeInMB");
        this.splitSizeInMB = splitSizeInMB;
        return this;
    }

    public TableReadSessionBuilder splitByParallelism(int splitParallelism) {
        Validator.checkInteger(splitParallelism, 1,"splitParallelism");
        this.splitParallelism = splitParallelism;
        return this;
    }

    public TableReadSessionBuilder bucketFilter(List<Integer> bucketFilter) {
        Validator.checkNotNull(bucketFilter, "readPartitions");
        this.bucketFilter = bucketFilter;
        return this;
    }

    public TableReadSessionBuilder splitAtBucketLevel(boolean splitAtBucketLevel) {
        this.splitAtBucketLevel = splitAtBucketLevel;
        return this;
    }

    public TableReadSessionBuilder filterExpressions(List<FilterExpression> filterExpressions) {
        Validator.checkNotNull(filterExpressions, "filterExpressions");
        this.filterExpressions = filterExpressions;
        return this;
    }

    public TableReadSessionBuilder functionCalls(List<FunctionCall> functionCalls) {
        Validator.checkNotNull(functionCalls, "functionCalls");
        this.functionCalls = functionCalls;
        return this;
    }

    public TableReadSession build() throws ClassNotFoundException {
        sanitizeBasicInfo();
        TableProvider tableProvider = ProviderRegistry.lookup(provider);
        ReadCapabilities capabilities = tableProvider.getReadCapabilities();
        TableReadSession session = tableProvider.createReadSession(
                project, table, tableSchema, readDataColumns, partitionSpecs, options);
        sanitizeExtendedInfo(capabilities, session);
        return session;
    }

    private void sanitizeBasicInfo() {
        markBuilt();

        if (readDataColumns == null) {
            throw new IllegalArgumentException("readDataColumns required");
        }

        if (readPartitions == null) {
            partitionSpecs = Collections.emptyList();
            partitionBucketFilters = Collections.emptyList();
        } else {
            Validator.checkList(readPartitions, "readPartitions");
            partitionSpecs = new ArrayList<>();
            partitionBucketFilters = new ArrayList<>();
            for (PartitionSpecWithBucketFilter p : readPartitions) {
                partitionSpecs.add(p.getPartitionSpec());
                partitionBucketFilters.add(p.getBucketFilter());
            }
            partitionSpecs = Collections.unmodifiableList(partitionSpecs);
            partitionBucketFilters = Collections.unmodifiableList(partitionBucketFilters);
        }
    }

    private void sanitizeExtendedInfo(ReadCapabilities capabilities, TableReadSession session) {
        session.setSplitBySize(splitSizeInMB);
        session.setSplitByParallelism(splitParallelism);

        if (bucketFilter == null) {
            bucketFilter = Collections.emptyList();
        } else {
            Validator.checkIntList(
                    bucketFilter, 1, 0, "bucketFilter");
            bucketFilter = Collections.unmodifiableList(new ArrayList<>(bucketFilter));
        }
        if (capabilities.supportBuckets()) {
            session.setBucketFilter(bucketFilter);
            session.setPartitionBucketFilters(partitionBucketFilters);
            session.setSplitAtBucketLevel(splitAtBucketLevel);
        } else if (needBucketSupport()) {
            throw new IllegalArgumentException(provider + " provider does not support buckets");
        }

        if (filterExpressions == null) {
            filterExpressions = Collections.emptyList();
        } else {
            Validator.checkList(filterExpressions, "filterExpressions");
            filterExpressions = Collections.unmodifiableList(new ArrayList<>(filterExpressions));
        }
        if (capabilities.supportPushDownFilters()) {
            session.setFilterExpressions(filterExpressions);
        } else if (!filterExpressions.isEmpty()) {
            throw new IllegalArgumentException(
                    provider + " provider does not support push down filters");
        }

        List<Attribute> functionCallResults;
        if (functionCalls == null) {
            functionCallResults = Collections.emptyList();
        } else {
            Validator.checkList(functionCalls, "functionCalls");
            functionCallResults = new ArrayList<>();
            for (FunctionCall f : functionCalls) {
                functionCallResults.add(f.toAttribute());
            }
            functionCallResults = Collections.unmodifiableList(functionCallResults);
        }
        if (capabilities.supportPushDownFunctionCalls()) {
            session.setFunctionCalls(functionCallResults);
        } else if (!functionCallResults.isEmpty()) {
            throw new IllegalArgumentException(
                    provider + " provider does not support push down function calls");
        }
    }

    private boolean needBucketSupport() {
        if (!bucketFilter.isEmpty() || splitAtBucketLevel) {
            return true;
        }
        for (List<Integer> f : partitionBucketFilters) {
            if (!f.isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
