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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.odps.table.tunnel.read;

import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.InputSplitAssigner;

import java.util.List;

public class TunnelInputSplitAssigner implements InputSplitAssigner {

    private TunnelInputSplit[] splits;

    public TunnelInputSplitAssigner(List<TunnelInputSplit> splits) {
        this.splits = splits.toArray(new TunnelInputSplit[0]);
    }

    @Override
    public int getSplitsCount() {
        return this.splits.length;
    }

    @Override
    public InputSplit getSplit(int index) {
        if (index >= this.splits.length) {
            throw new IllegalArgumentException("Index of input split exceed max count");
        }
        return this.splits[index];
    }

    @Override
    public InputSplit[] getAllSplits() {
        return this.splits;
    }

}
