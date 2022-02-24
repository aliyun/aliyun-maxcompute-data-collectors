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

public final class ReadCapabilities {

    private final boolean supportBuckets;
    private final boolean supportPushDownFilters;
    private final boolean supportPushDownFunctionCalls;

    public ReadCapabilities(boolean supportBuckets,
                            boolean supportPushDownFilters,
                            boolean supportPushDownFunctionCalls) {
        this.supportBuckets = supportBuckets;
        this.supportPushDownFilters = supportPushDownFilters;
        this.supportPushDownFunctionCalls = supportPushDownFunctionCalls;
    }

    public boolean supportBuckets() {
        return supportBuckets;
    }

    public boolean supportPushDownFilters() {
        return supportPushDownFilters;
    }

    public boolean supportPushDownFunctionCalls() {
        return supportPushDownFunctionCalls;
    }

    // TODO: support split mode and data columns
}
