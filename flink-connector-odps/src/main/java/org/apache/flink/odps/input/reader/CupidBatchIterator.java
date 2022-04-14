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

package org.apache.flink.odps.input.reader;

import com.aliyun.odps.Column;
import com.aliyun.odps.cupid.table.v1.reader.SplitReader;
import com.aliyun.odps.cupid.table.v1.reader.SplitReaderBuilder;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.data.Record;
import org.apache.flink.odps.input.OdpsInputSplit;
import org.apache.flink.odps.schema.OdpsTableSchema;
import org.apache.flink.odps.util.OdpsTypeConverter;
import org.apache.flink.odps.util.OdpsUtils.RecordType;
import org.apache.flink.odps.vectorized.ColumnarReadBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


public class CupidBatchIterator<T> implements NextIterator<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CupidBatchIterator.class);

    private final ColumnarReadBatch currentBatch;
    private final Column[] fullColumns;
    private final OdpsTypeConverter[] typeConverters;
    private final RecordType recordType;
    private SplitReader<ColDataBatch> splitReader;
    private Iterator<Record> currentRowIterator;
    private T reuse;

    public CupidBatchIterator(OdpsInputSplit split,
                              OdpsTableSchema odpsTableSchema,
                              String[] selectedColumns,
                              RecordType recordType,
                              int batchSize) throws Exception {
        try {
            splitReader = new SplitReaderBuilder(split.inputSplit)
                    .buildColDataReader(batchSize);
        } catch (Throwable throwable) {
            throw new Exception(throwable);
        }
        this.fullColumns = new Column[selectedColumns.length];
        this.typeConverters = new OdpsTypeConverter[selectedColumns.length];

        for (int i = 0; i < fullColumns.length; ++i) {
            this.fullColumns[i] = new Column(odpsTableSchema.getColumn(selectedColumns[i]).getName(),
                    odpsTableSchema.getColumn(selectedColumns[i]).getTypeInfo());
            this.typeConverters[i] = OdpsTypeConverter.valueOf(this.fullColumns[i].getType().name());

        }
        this.currentBatch = new ColumnarReadBatch(fullColumns, split.inputSplit.getPartitionSpec());
        this.recordType = recordType;
        LOG.info("use batch iterator");
    }

    @Override
    public void close() throws IOException {
        if (splitReader != null) {
            splitReader.close();
        }
        splitReader = null;
        currentRowIterator = null;
    }

    @Override
    public boolean hasNext() {
        if (currentRowIterator != null && currentRowIterator.hasNext()) {
            return true;
        }
        if (!splitReader.hasNext()) {
            return false;
        }
        currentBatch.updateColumnBatch(splitReader.next());
        currentRowIterator = currentBatch.rowIterator();
        return currentRowIterator.hasNext();
    }

    @Override
    public T next() {
        return buildReturnType(currentRowIterator.next());
    }

    private T buildReturnType(Record record) {
        // TODO: record may be null
        switch (recordType) {
            case FLINK_ROW_DATA:
                return (T) buildFlinkRowData(record, fullColumns, typeConverters);
            case FLINK_TUPLE:
                return (T) buildFlinkTuple(reuse, record, fullColumns, typeConverters);
            case FLINK_ROW:
                return (T) buildFlinkRow(reuse, record, fullColumns, typeConverters);
            default:
                return (T) buildFlinkPojo(reuse, record, fullColumns);
        }
    }

    @Override
    public void setReuse(T reuse) {
        this.reuse = reuse;
    }
}