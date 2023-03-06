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

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.TableReadSessionProvider;

import java.io.IOException;

public class TunnelReadSessionProviderImpl implements TableReadSessionProvider {

    @Override
    public String identifier() {
        return "tunnel";
    }

    @Override
    public TableBatchReadSession createBatchReadSession(TableReadSessionBuilder builder) throws IOException {
        if (builder.getSessionId() == null) {
            return new TunnelTableBatchReadSession(builder.getIdentifier(),
                    builder.getRequiredPartitions(),
                    builder.getRequiredDataColumns(),
                    builder.getRequiredPartitionColumns(),
                    builder.getRequiredBucketIds(),
                    builder.getSplitOptions(),
                    builder.getArrowOptions(),
                    builder.getSettings());
        } else {
            throw new UnsupportedOperationException("Unsupported reload tunnel read session!");
        }
    }
}
