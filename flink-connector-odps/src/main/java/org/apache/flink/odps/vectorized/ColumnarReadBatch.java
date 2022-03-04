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

package org.apache.flink.odps.vectorized;

import com.aliyun.odps.Column;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.data.Record;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public class ColumnarReadBatch {

    private ColDataBatch colDataBatch;
    private final MutableColumnarRow row;

    public ColumnarReadBatch(Column[] fullColumns, Map<String, String> partitionSpec) {
        this.row = new MutableColumnarRow(fullColumns, partitionSpec);
    }

    public void updateColumnBatch(ColDataBatch colDataBatch) {
        this.colDataBatch = colDataBatch;
        this.row.updateColDataBatch(colDataBatch);
    }

    /**
     * Returns an iterator over the rows in this batch.
     */
    public Iterator<Record> rowIterator() {
        final int maxRows = colDataBatch.getRowCount();
        return new Iterator<Record>() {
            int rowId = 0;

            @Override
            public boolean hasNext() {
                return rowId < maxRows;
            }

            @Override
            public Record next() {
                if (rowId >= maxRows) {
                    throw new NoSuchElementException();
                }
                row.rowId = rowId++;
                return row.getRow();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
