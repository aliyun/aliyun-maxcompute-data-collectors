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

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;

import java.util.Objects;

public class TunnelInputSplit extends RowRangeInputSplit {

    private final PartitionSpec partitionSpec;

    public TunnelInputSplit(String scanId,
                            long startIndex,
                            long numRecord,
                            PartitionSpec partitionSpec) {
        super(scanId, startIndex, numRecord);
        this.partitionSpec = partitionSpec;
    }

    public PartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.partitionSpec);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof TunnelInputSplit && super.equals(obj)) {
            TunnelInputSplit other = (TunnelInputSplit) obj;
            return Objects.equals(this.partitionSpec, other.partitionSpec);
        } else {
            return false;
        }
    }
}
