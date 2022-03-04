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
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsTypeConverter;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RowBlockWriter<T> implements BlockWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RowBlockWriter.class);

    private final ArrayRecord odpsRecord;
    private final Column[] columns;
    private final OdpsTypeConverter[] odpsTypeConverters;
    private TypeComparator<T> typeComparator;

    protected final OdpsWriteOptions options;
    protected FileWriter<ArrayRecord> fileWriter;
    protected RetryStrategy writeRetry;

    public RowBlockWriter(Column[] cols,
                          FileWriter<ArrayRecord> fileWriter,
                          OdpsWriteOptions options) {
        Preconditions.checkNotNull(cols, "columns cannot be null");
        Preconditions.checkNotNull(fileWriter, "file writer cannot be null");
        this.columns = cols;
        this.fileWriter = fileWriter;
        this.options = options;
        this.odpsRecord = new ArrayRecord(columns);
        this.odpsTypeConverters = new OdpsTypeConverter[cols.length];
        for(int i = 0; i < odpsRecord.getColumnCount(); ++i) {
            odpsTypeConverters[i] =
                    OdpsTypeConverter.valueOf(odpsRecord.getColumns()[i].getTypeInfo().getOdpsType().name());
        }
        if (options.getWriteMaxRetries() > 0) {
            this.writeRetry = new RetryStrategy(options.getWriteMaxRetries(), 1);
        } else {
            this.writeRetry = new RetryStrategy(3, 1);
        }
    }

    @Override
    public void updateFileWriter(FileWriter fileWriter, int blockId) {
        this.fileWriter = fileWriter;
    }

    @Override
    public void write(T rec) throws IOException {
        for (int i = 0; i < odpsRecord.getColumnCount(); ++i) {
            odpsRecord.set(i, null);
        }
        buildOdpsRecord(odpsRecord, rec);
        tryWrite();
    }

    private void tryWrite() throws IOException {
        writeRetry.reset();
        while (true) {
            try {
                fileWriter.write(odpsRecord);
                break;
            } catch (Exception exception) {
                LOG.error(String.format("Write error, retry times = %d",
                        writeRetry.getAttempts()), exception);
                try {
                    writeRetry.onFailure(exception);
                } catch (RetryExceedLimitException | InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        fileWriter.close();
    }

    @Override
    public void commit() throws IOException {
        fileWriter.commit();
    }

    @Override
    public long getBytesWritten() {
        return fileWriter.getBytesWritten();
    }

    @Override
    public long getRowsWritten() {
        return fileWriter.getRowsWritten();
    }

    private void buildOdpsRecord(Record odpsRecord, T record) {
        Object sourceValue;
        Object[] keyArray = null;
        for (int i = 0; i < odpsRecord.getColumnCount(); ++i) {
            if (record instanceof Tuple) {
                sourceValue = ((Tuple) record).getField(i);
            } else if (record instanceof Row) {
                sourceValue = ((Row) record).getField(i);
            } else {
                if (typeComparator == null) {
                    this.typeComparator = OdpsUtils.buildTypeComparator(record,
                            Arrays.stream(columns).map(Column::getName).collect(Collectors.toList()));
                }
                if (i == 0) {
                    keyArray = new Object[this.columns.length];
                    typeComparator.extractKeys(record, keyArray, 0);
                }
                sourceValue = keyArray[i];
            }
            if (sourceValue == null) {
                odpsRecord.set(i, null);
                continue;
            }
            Object odpsField = odpsTypeConverters[i].toOdpsField(sourceValue,
                    odpsRecord.getColumns()[i].getTypeInfo());
            odpsRecord.set(i, odpsField);
        }
    }
}
