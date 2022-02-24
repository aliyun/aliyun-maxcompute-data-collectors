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

import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class BucketFileInfo {

    private Map<Integer, List<Integer>> bucketFileIndices;
    private Map<String, String> partitionSpec;

    public BucketFileInfo(Map<Integer, List<Integer>> bucketFileIndices) {
        Validator.checkBucketFileIndices(bucketFileIndices, "bucketFileIndices");
        this.bucketFileIndices = bucketFileIndices;
        this.partitionSpec = Collections.emptyMap();
    }

    public BucketFileInfo(Map<Integer, List<Integer>> bucketFileIndices,
                          Map<String, String> partitionSpec) {
        Validator.checkBucketFileIndices(bucketFileIndices, "bucketFileIndices");
        Validator.checkMap(partitionSpec, "partitionSpec");
        this.bucketFileIndices = bucketFileIndices;
        this.partitionSpec = partitionSpec;
    }

    public Map<Integer, List<Integer>> getBucketFileIndices() {
        return bucketFileIndices;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }
}
