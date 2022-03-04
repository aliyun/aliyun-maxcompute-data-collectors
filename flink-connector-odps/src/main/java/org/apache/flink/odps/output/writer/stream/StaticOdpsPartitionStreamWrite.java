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
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.FileWriterBuilder;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.flink.odps.output.writer.OdpsStreamWrite;
import org.apache.flink.odps.output.writer.OdpsTableWrite;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.odps.util.Constants.ODPS_WRITER_STREAMING_ENABLE;

public class StaticOdpsPartitionStreamWrite<T> extends OdpsTableWrite<T>
        implements OdpsStreamWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticOdpsPartitionStreamWrite.class);

    protected transient StreamWriter<T> streamWriter;
    protected String currentPartition;

    public StaticOdpsPartitionStreamWrite(OdpsConf odpsConf,
                                          String projectName,
                                          String tableName,
                                          String partition,
                                          OdpsWriteOptions options) {
        super(odpsConf, projectName, tableName, partition, false, options);
        this.currentPartition = staticPartition;
    }

    @Override
    public void initWriteSession() throws IOException {
        Options options = OdpsUtils.getOdpsOptions(odpsConf);
        options.put(ODPS_WRITER_STREAMING_ENABLE, "true");
        createWriteSession(options, staticPartition);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            Preconditions.checkNotNull(writeSessionInfo, "Write session cannot be null!");
            if (useBatch) {
                throw new UnsupportedOperationException();
            } else {
                FileWriter<ArrayRecord> writer;
                if (isPartitioned) {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .partitionSpec(OdpsUtils.getPartitionSpecKVMap(new PartitionSpec(currentPartition)))
                            .buildRecordWriter();
                } else {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .buildRecordWriter();
                }
                streamWriter = new RowStreamWriter<T>(
                        getTableSchema().getColumns().toArray(new Column[0]),
                        writer,
                        writeOptions) {
                };
            }
        } catch (ClassNotFoundException e) {
            LOG.error("Fail to init odps file writer!", e);
            throw new IOException("Fail to init odps file writer!", e);
        }
    }

    @Override
    public void writeRecord(T record) throws IOException {
        if (streamWriter != null) {
            this.streamWriter.write(record);
        }
    }

    @Override
    public void flush() throws IOException {
        if (streamWriter != null) {
            this.streamWriter.flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void commitWriteSession() throws IOException {
        return;
    }

    @Override
    public boolean isIdle() {
        if (streamWriter != null) {
            return streamWriter.isIdle();
        }
        return true;
    }

    @Override
    public long getFlushInterval() {
        if (streamWriter != null) {
            return streamWriter.getFlushInterval();
        }
        return Long.MAX_VALUE;
    }
}
