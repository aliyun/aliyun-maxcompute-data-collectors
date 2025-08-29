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

package org.apache.spark.sql.odps.table.tunnel.write;

import com.aliyun.odps.table.write.*;

import java.io.IOException;

public class TunnelWriteSessionProviderImpl implements TableWriteSessionProvider {

    @Override
    public String identifier() {
        return "tunnel";
    }

    @Override
    public TableBatchWriteSession createBatchWriteSession(TableWriteSessionBuilder builder) throws IOException {
        return new TunnelTableBatchWriteSession(builder);
    }

    @Override
    public TableStreamingWriteSession createStreamingWriteSession(TableWriteSessionBuilder builder) throws IOException {
        throw new UnsupportedOperationException("Cannot build streaming write session.");
    }

    @Override
    public TableUpsertSession createUpsertSession(TableWriteSessionBuilder builder) throws IOException {
        throw new UnsupportedOperationException("Cannot build upsert session.");
    }
}
