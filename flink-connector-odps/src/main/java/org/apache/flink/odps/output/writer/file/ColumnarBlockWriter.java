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

package org.apache.flink.odps.output.writer.file;

import com.aliyun.odps.Column;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.adaptor.ColDataRowWriter;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.vectorized.ColDataRowImpl;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

public class ColumnarBlockWriter<T> implements BlockWriter<T> {

    private final ColDataRowImpl<T> colDataRow;
    private final int batchSize;
    private final Column[] columns;
    private ColDataRowWriter colDataRowWriter;

    public ColumnarBlockWriter(Column[] cols,
                               FileWriter<ColDataBatch> fileWriter,
                               OdpsWriteOptions options,
                               int batchSize) {
        Preconditions.checkNotNull(cols, "columns cannot be null");
        Preconditions.checkNotNull(fileWriter, "file writer cannot be null");
        this.columns = cols;
        this.batchSize = batchSize;
        this.colDataRow = new ColDataRowImpl<>(cols);
        this.colDataRowWriter = new ColDataRowWriter(cols, fileWriter, batchSize);
        // TODO: OdpsWriteOptions
    }

    @Override
    public void updateFileWriter(FileWriter fileWriter, int blockId) {
        this.colDataRowWriter = new ColDataRowWriter(columns, fileWriter, batchSize);
    }

    @Override
    public void write(T rec) throws IOException {
        this.colDataRow.setRow(rec);
        this.colDataRowWriter.insert(this.colDataRow);
    }

    @Override
    public void close() throws IOException {
        this.colDataRowWriter.close();
    }

    @Override
    public void commit() throws IOException {
        this.colDataRowWriter.commit();
    }

    @Override
    public long getBytesWritten() {
        return this.colDataRowWriter.getBytesWritten();
    }

    @Override
    public long getRowsWritten() {
        return this.colDataRowWriter.getRowsWritten();
    }
}
