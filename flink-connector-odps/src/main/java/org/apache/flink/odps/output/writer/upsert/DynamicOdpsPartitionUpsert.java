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

package org.apache.flink.odps.output.writer.upsert;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.stream.TablePartitionAssigner;
import org.apache.flink.odps.output.writer.*;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.odps.util.OdpsUtils.getPartitionComputer;

public class DynamicOdpsPartitionUpsert extends OdpsTableWrite<Row>
        implements OdpsUpsert<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicOdpsPartitionUpsert.class);
    private final PartitionAssigner<Row> partitionAssigner;
    private final transient Map<String, OdpsUpsert<Row>> odpsPartitionWriterMap;
    private int taskNumber;
    private int numTasks;

    public DynamicOdpsPartitionUpsert(OdpsConf odpsConf,
                                      String projectName,
                                      String tableName,
                                      String partition,
                                      OdpsWriteOptions options,
                                      PartitionAssigner<Row> partitionAssigner) {
        super(odpsConf, projectName, tableName, partition, false, options);
        this.partitionAssigner = partitionAssigner == null ?
                new TablePartitionAssigner<>(getPartitionComputer(getTableSchema(), staticPartition)) :
                partitionAssigner;
        this.odpsPartitionWriterMap = new HashMap<>();
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void upsert(Row record) throws IOException {
        getUpsert(record).upsert(record);
    }

    @Override
    public void delete(Row record) throws IOException {
        getUpsert(record).delete(record);
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
    public void flush() throws IOException {
        // TODO: flush
    }

    private OdpsUpsert<Row> getUpsert(Row record) throws IOException {
        String partition = this.partitionAssigner.getPartitionSpec(record, writerContext);
        OdpsUpsert<Row> singleOdpsPartitionWriter = odpsPartitionWriterMap.get(partition);
        if (singleOdpsPartitionWriter == null) {
            singleOdpsPartitionWriter = createSingleOdpsPartitionWriter(partition);
        }
        return singleOdpsPartitionWriter;
    }

    @Override
    public void close() throws IOException {
        for (Map.Entry<String, OdpsUpsert<Row>> odpsWriterEntry :
                odpsPartitionWriterMap.entrySet()) {
            LOG.info("Try to close odps partition" + odpsWriterEntry.getKey());
            odpsWriterEntry.getValue().close();
        }
        odpsPartitionWriterMap.clear();
    }

    private OdpsUpsert<Row> createSingleOdpsPartitionWriter(String partition) throws IOException {
        if (odpsPartitionWriterMap.size() >= writeOptions.getDynamicPartitionLimit()) {
            // TODO: writer close
            throw new IOException("Too many dynamic partitions: "
                    + odpsPartitionWriterMap.size()
                    + ", which exceeds the size limit: " + writeOptions.getDynamicPartitionLimit());
        }
        OdpsUpsert<Row> singleOdpsPartitionWriter = OdpsWriteFactory.createOdpsUpsert(
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
