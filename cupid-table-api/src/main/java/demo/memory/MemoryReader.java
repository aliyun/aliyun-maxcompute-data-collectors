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

import com.aliyun.odps.cupid.table.v1.reader.SplitReader;
import com.aliyun.odps.data.ArrayRecord;

import java.io.IOException;

class MemoryReader implements SplitReader<ArrayRecord> {

    private final String project;
    private final String table;
    private final int index;

    private ArrayRecord[] file;
    private int cursor;

    MemoryReader(String project, String table, int index) {
        this.project = project;
        this.table = table;
        this.index = index;
        file = MemoryStore.getTable(project, table).read(index);
    }

    @Override
    public void close() throws IOException {
        file = null;
    }

    @Override
    public long getBytesRead() {
        return -1;
    }

    @Override
    public long getRowsRead() {
        return cursor;
    }

    @Override
    public boolean hasNext() {
        return cursor < file.length;
    }

    @Override
    public ArrayRecord next() {
        return file[cursor++];
    }
}
