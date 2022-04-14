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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.stream.TablePartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsStreamWrite;
import org.apache.flink.odps.output.writer.OdpsTableWrite;
import org.apache.flink.odps.output.writer.OdpsWriteFactory;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.odps.util.OdpsUtils.getPartitionComputer;

public class DynamicOdpsPartitionStreamWrite<T> extends OdpsTableWrite<T>
        implements OdpsStreamWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicOdpsPartitionStreamWrite.class);
    private final PartitionAssigner<T> partitionAssigner;
    private final transient Map<String, OdpsStreamWrite<T>> odpsPartitionWriterMap;
    private int taskNumber;
    private int numTasks;

    public DynamicOdpsPartitionStreamWrite(OdpsConf odpsConf,
                                           String projectName,
                                           String tableName,
                                           String partition,
                                           OdpsWriteOptions options,
                                           PartitionAssigner<T> partitionAssigner) {
        super(odpsConf, projectName, tableName, partition, false, options);
        this.partitionAssigner = partitionAssigner == null ?
                new TablePartitionAssigner<>(getPartitionComputer(getTableSchema(), staticPartition)) :
                partitionAssigner;
        this.odpsPartitionWriterMap = new HashMap<>();
    }

    @Override
    public void initWriteSession() throws IOException {
        return;
    }

    @Override
    public void commitWriteSession() throws IOException {
        return;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    @Override
    public void writeRecord(T record) throws IOException {
        String partition = this.partitionAssigner.getPartitionSpec(record, writerContext);
        OdpsStreamWrite<T> singleOdpsPartitionWriter = odpsPartitionWriterMap.get(partition);
        if (singleOdpsPartitionWriter == null) {
            singleOdpsPartitionWriter = createSingleOdpsPartitionWriter(partition);
        }
        singleOdpsPartitionWriter.writeRecord(record);
    }

    private OdpsStreamWrite<T> createSingleOdpsPartitionWriter(String partition) throws IOException {
        if (odpsPartitionWriterMap.size() >= writeOptions.getDynamicPartitionLimit()) {
            String partKey = null;
            long idleTime = 0L;
            for (Map.Entry<String, OdpsStreamWrite<T>> odpsWriterEntry : odpsPartitionWriterMap.entrySet()) {
                if (odpsWriterEntry.getValue().isIdle() && odpsWriterEntry.getValue().getFlushInterval() > idleTime) {
                    idleTime = odpsWriterEntry.getValue().getFlushInterval();
                    partKey = odpsWriterEntry.getKey();
                }
            }
            if (partKey == null) {
                throw new IOException("Too many dynamic partitions: "
                        + odpsPartitionWriterMap.size()
                        + ", which exceeds the size limit: " + writeOptions.getDynamicPartitionLimit());
            }
            this.odpsPartitionWriterMap.remove(partKey);
        }
        OdpsStreamWrite<T> singleOdpsPartitionWriter = OdpsWriteFactory.createOdpsStreamWrite(
                odpsConf,
                projectName,
                tableName,
                partition,
                false,
                false,
                writeOptions,
                null);
        singleOdpsPartitionWriter.initWriteSession();
        singleOdpsPartitionWriter.open(taskNumber, numTasks);
        this.odpsPartitionWriterMap.put(partition, singleOdpsPartitionWriter);
        LOG.info("Create new odps writer for dynamic partitions");
        return singleOdpsPartitionWriter;
    }

    @Override
    public void flush() throws IOException {
        for (Map.Entry<String, OdpsStreamWrite<T>> odpsWriterEntry :
                odpsPartitionWriterMap.entrySet()) {
            odpsWriterEntry.getValue().flush();
        }
    }

    @Override
    public boolean isIdle() {
        for (Map.Entry<String, OdpsStreamWrite<T>> odpsWriterEntry : odpsPartitionWriterMap.entrySet()) {
            if (!odpsWriterEntry.getValue().isIdle()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getFlushInterval() {
        throw new UnsupportedOperationException("Dynamic odps partition writer not support get flush interval");
    }

    @Override
    protected void checkPartition(String partitionSpec) throws IOException {
        try {
            Table table = getTableMetaProvider().getTable(projectName, tableName);
            this.isPartitioned = table.isPartitioned();
            Preconditions.checkArgument(this.isPartitioned,
                    "Table " + tableName + " is not partitioned");
            OdpsUtils.checkPartition(partitionSpec, getTableSchema());
            this.staticPartition = partitionSpec;
        } catch (OdpsException e) {
            throw new IOException("check partition failed.", e);
        }
    }
}
