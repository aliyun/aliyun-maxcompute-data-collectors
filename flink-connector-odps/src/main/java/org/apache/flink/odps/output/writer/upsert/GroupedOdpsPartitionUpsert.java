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

import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.stream.TablePartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.odps.util.OdpsUtils.getPartitionComputer;

public class GroupedOdpsPartitionUpsert extends StaticOdpsPartitionUpsert {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedOdpsPartitionUpsert.class);
    private final PartitionAssigner<Row> partitionAssigner;
    private int taskNumber;
    private int numTasks;

    public GroupedOdpsPartitionUpsert(OdpsConf odpsConf,
                                      String projectName,
                                      String tableName,
                                      String partition,
                                      OdpsWriteOptions options,
                                      PartitionAssigner<Row> partitionAssigner) {
        super(odpsConf, projectName, tableName, partition, options);
        this.partitionAssigner = partitionAssigner == null ?
                new TablePartitionAssigner<>(getPartitionComputer(getTableSchema(), staticPartition)) :
                partitionAssigner;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    public void upsert(Row record) throws IOException {
        nextPartition(record);
        super.upsert(record);
    }

    @Override
    public void delete(Row record) throws IOException {
        nextPartition(record);
        super.delete(record);
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
    protected void checkPartition(String partitionSpec) {
        Preconditions.checkArgument(this.isPartitioned,
                "Table " + tableName + " is not partitioned");
        OdpsUtils.checkPartition(partitionSpec, getTableSchema());
        this.staticPartition = partitionSpec;
        this.currentPartition = "";
    }

    private void nextPartition(Row record) throws IOException {
        String partition = this.partitionAssigner.getPartitionSpec(record, writerContext);
        if (!partition.equals(currentPartition)) {
            try {
                if (isLocal) {
                    if (!currentPartition.isEmpty()) {
                        super.close();
                    }
                } else {
                    // TODO: cluster mode support grouped writer
                    throw new UnsupportedOperationException();
                }
                this.currentPartition = partition;
                createPartitionIfNeeded(currentPartition);
                super.initWriteSession();
                super.open(taskNumber, numTasks);
            } catch (IOException e) {
                LOG.error("change partition error, current partition: " + currentPartition);
                throw e;
            }
        }
    }
}
