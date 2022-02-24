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

package demo.memory;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.util.TableUtils;
import com.aliyun.odps.cupid.table.v1.writer.BucketFileInfo;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class MemoryWriteSession extends TableWriteSession {

    private WriteSessionInfo writeSessionInfo;

    MemoryWriteSession(String project,
                       String table,
                       TableSchema tableSchema,
                       Map<String, String> partitionSpec,
                       boolean overwrite) {
        super(project, table, tableSchema, partitionSpec, overwrite);
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }

    MemoryWriteSession(WriteSessionInfo writeSessionInfo) {
        super(writeSessionInfo.getProject(),
                writeSessionInfo.getTable(),
                TableUtils.toTableSchema(writeSessionInfo),
                writeSessionInfo.getPartitionSpec(),
                writeSessionInfo.isOverwrite());
        this.writeSessionInfo = writeSessionInfo;
    }

    @Override
    public WriteSessionInfo getOrCreateSessionInfo() {
        if (writeSessionInfo != null) {
            return writeSessionInfo;
        }
        return new WriteSessionInfo(project, table, Utils.toAttributes(tableSchema.getColumns()),
                Utils.toAttributes(tableSchema.getPartitionColumns()),
                partitionSpec) {
            @Override
            public String getProvider() {
                return "memory";
            }
        };
    }

    @Override
    public void commitTable() throws IOException {
        if (overwrite) {
            throw new IllegalArgumentException("overwrite");
        }
    }

    @Override
    public void commitTableWithPartitions(List<Map<String, String>> partitionSpecs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitClusteredTable(List<BucketFileInfo> partitionSpecs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup() throws IOException {
    }
}
