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

import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.*;

public final class PartitionSpecWithBucketFilter {

    private final Map<String, String> partitionSpec;
    private List<Integer> bucketFilter;

    public PartitionSpecWithBucketFilter(Map<String, String> partitionSpec) {
        Validator.checkMap(partitionSpec, "partitionSpec");
        this.partitionSpec = partitionSpec;
    }

    public PartitionSpecWithBucketFilter(Map<String, String> partitionSpec, List<Integer> bucketFilter) {
        Validator.checkMap(partitionSpec, "partitionSpec");
        Validator.checkIntList(bucketFilter, 1, 0, "bucketFilter");
        this.partitionSpec = partitionSpec;
        this.bucketFilter = bucketFilter;
    }

    Map<String, String> getPartitionSpec() {
        return Collections.unmodifiableMap(new HashMap<>(partitionSpec));
    }

    List<Integer> getBucketFilter() {
        if (bucketFilter == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(new ArrayList<>(bucketFilter));
        }
    }
}
