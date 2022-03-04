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
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.FileWriterBuilder;
import org.apache.flink.odps.output.writer.OdpsTableWrite;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.odps.util.Constants.*;

public class StaticOdpsPartitionWrite<T> extends OdpsTableWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticOdpsPartitionWrite.class);
    protected transient BlockWriter<T> blockWriter;
    protected String currentPartition;
    protected int taskNumber;
    protected int numTasks;

    public StaticOdpsPartitionWrite(OdpsConf odpsConf,
                                    String projectName,
                                    String tableName,
                                    String partition,
                                    boolean isOverwrite,
                                    OdpsWriteOptions options) {
        super(odpsConf, projectName, tableName, partition, isOverwrite, options);
        this.currentPartition = staticPartition;
    }

    @Override
    public void initWriteSession() throws IOException {
        Options options = OdpsUtils.getOdpsOptions(odpsConf);
        createWriteSession(options, currentPartition);
        sessionCreateTime = System.currentTimeMillis();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            Preconditions.checkNotNull(writeSessionInfo, "Write session cannot be null!");
            this.taskNumber = taskNumber;
            this.numTasks = numTasks;
            FileWriter writer;
            if (useBatch) {
                if (isPartitioned) {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .partitionSpec(OdpsUtils.getPartitionSpecKVMap(new PartitionSpec(currentPartition)))
                            .buildColDataWriter();
                } else {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .buildColDataWriter();
                }
                if (blockWriter == null) {
                    // TODO: use write options
                    int batchSize = odpsConf.getPropertyOrDefault(ODPS_VECTORIZED_BATCH_SIZE,
                            DEFAULT_ODPS_VECTORIZED_BATCH_SIZE);
                    blockWriter = new ColumnarBlockWriter<T>(
                            getTableSchema().getColumns().toArray(new Column[0]),
                            writer,
                            writeOptions,
                            batchSize);
                }
            } else {
                if (this.writeSessionInfo.getOptions().getOrDefault(ODPS_WRITER_BUFFER_ENABLE, false)) {
                    LOG.info("Create buffered odps writer with shares:" + numTasks);
                    this.writeSessionInfo.getOptions().put(ODPS_WRITER_BUFFER_SHARES, String.valueOf(numTasks));
                }
                if (isPartitioned) {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .partitionSpec(OdpsUtils.getPartitionSpecKVMap(new PartitionSpec(currentPartition)))
                            .buildRecordWriter();
                } else {
                    writer = new FileWriterBuilder(writeSessionInfo, taskNumber)
                            .buildRecordWriter();
                }
                if (blockWriter == null) {
                    blockWriter = new RowBlockWriter<T>(
                            getTableSchema().getColumns().toArray(new Column[0]),
                            writer,
                            writeOptions);
                }
            }
            blockWriter.updateFileWriter(writer, taskNumber);
        } catch (ClassNotFoundException e) {
            LOG.error("Fail to init odps file writer!", e);
            throw new IOException("Fail to init odps file writer!", e);
        }
    }

    @Override
    public void writeRecord(T record) throws IOException {
        if (blockWriter == null) {
            throw new IOException("Odps writer is closed");
        }
        this.blockWriter.write(record);
    }

    @Override
    public void close() throws IOException {
        if (blockWriter != null) {
            try {
                blockWriter.close();
                blockWriter.commit();
            } catch (Throwable e) {
                LOG.error("Failed to close odps writer!", e);
                throw new IOException(e);
            }
            blockWriter = null;
            LOG.info("Finish to close odps writer");
        }
    }

    @Override
    public void commitWriteSession() throws IOException {
        rebuildWriteSession();
        try {
            tableWriteSession.commitTable();
            tableWriteSession.cleanup();
        } catch (Throwable e) {
            LOG.error("Failed to commit odps write session!", e);
            throw new IOException(e);
        } finally {
            tableWriteSession = null;
        }
    }
}
