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

package org.apache.flink.odps.output.writer.stream;

import com.aliyun.odps.Column;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.output.writer.file.RowBlockWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RowStreamWriter<T> extends RowBlockWriter<T>
        implements StreamWriter<T> {

    private static final Logger LOG = LoggerFactory.getLogger(RowStreamWriter.class);
    private final RetryStrategy flushRetry;
    private long lastFlushTime;

    public RowStreamWriter(Column[] cols,
                           FileWriter<ArrayRecord> fileWriter,
                           OdpsWriteOptions options) {
        super(cols, fileWriter, options);
        if (options.getWriteMaxRetries() > 0) {
            this.flushRetry = new RetryStrategy(options.getWriteMaxRetries(), 3);
        } else {
            this.flushRetry = new RetryStrategy(3, 3);
        }
        this.lastFlushTime = System.currentTimeMillis();
        this.fileWriter = fileWriter;
    }

    @Override
    public void write(T rec) throws IOException {
        super.write(rec);
        if (options.getBufferFlushMaxRows() > 0
                && fileWriter.getBufferRows() > options.getBufferFlushMaxRows()) {
            flush();
        } else if (options.getBufferFlushMaxSizeInBytes() > 0L
                && fileWriter.getBufferBytes() > options.getBufferFlushMaxSizeInBytes()) {
            flush();
        }
    }

    @Override
    public void flush() throws IOException {
        if (!isIdle()) {
            flushRetry.reset();
            while (true) {
                try {
                    fileWriter.flush();
                    break;
                } catch (Exception exception) {
                    LOG.error(String.format("Flush error, retry times = %d",
                            flushRetry.getAttempts()), exception);
                    try {
                        flushRetry.onFailure(exception);
                    } catch (RetryExceedLimitException | InterruptedException e) {
                        throw new IOException(e);
                    }
                }
            }
            this.lastFlushTime = System.currentTimeMillis();
        }
    }

    @Override
    public boolean isIdle() {
        return fileWriter.getBufferRows() == 0;
    }

    @Override
    public long getFlushInterval() {
        return System.currentTimeMillis() - this.lastFlushTime;
    }
}
