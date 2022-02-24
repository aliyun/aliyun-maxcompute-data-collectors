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
import com.aliyun.odps.cupid.table.v1.reader.InputSplit;
import com.aliyun.odps.cupid.table.v1.reader.RequiredSchema;
import com.aliyun.odps.cupid.table.v1.reader.TableReadSession;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class MemoryReadSession extends TableReadSession {

    private InputSplit[] inputSplits;

    MemoryReadSession(String project,
                      String table,
                      TableSchema tableSchema,
                      RequiredSchema readDataColumns,
                      List<Map<String, String>> partitionSpecs) {
        super(project, table, tableSchema, readDataColumns, partitionSpecs);
    }

    @Override
    public InputSplit[] getOrCreateInputSplits(int splitSizeInMB) throws IOException {
        if (inputSplits == null) {
            if (tableSchema == null) {
                tableSchema = new TableSchema();
            }

            Map<String, String> emptyPartSpec = Collections.emptyMap();
            MemoryStore.Table memTable = MemoryStore.getTable(project, table);
            inputSplits = new InputSplit[memTable.getFileCount()];
            for (int i = 0; i < inputSplits.length; i++) {
                inputSplits[i] = new MemoryStore.IndexSplit(project, table,
                        Utils.toAttributes(tableSchema.getColumns()),
                        Utils.toAttributes(tableSchema.getPartitionColumns()),
                        readDataColumns.toList(), emptyPartSpec, i);
            }
        }
        return inputSplits;
    }

    @Override
    public InputSplit[] getOrCreateInputSplits() throws IOException {
        throw new UnsupportedOperationException();
    }
}
