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

import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.stream.TablePartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.odps.util.OdpsUtils.getPartitionComputer;

public class GroupedOdpsPartitionStreamWrite<T> extends StaticOdpsPartitionStreamWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(GroupedOdpsPartitionStreamWrite.class);
    private final PartitionAssigner<T> partitionAssigner;
    private int taskNumber;
    private int numTasks;

    public GroupedOdpsPartitionStreamWrite(OdpsConf odpsConf,
                                           String projectName,
                                           String tableName,
                                           String partition,
                                           OdpsWriteOptions options,
                                           PartitionAssigner<T> partitionAssigner) {
        super(odpsConf, projectName, tableName, partition, options);
        this.partitionAssigner = partitionAssigner == null ?
                new TablePartitionAssigner<>(getPartitionComputer(getTableSchema(), staticPartition)) :
                partitionAssigner;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        Preconditions.checkNotNull(writeSessionInfo, "Write session cannot be null!");
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
    }

    @Override
    public void writeRecord(T record) throws IOException {
        String partition = this.partitionAssigner.getPartitionSpec(record, writerContext);
        if (!partition.equals(currentPartition)) {
            try {
                if (isLocal) {
                    if (!currentPartition.isEmpty()) {
                        super.flush();
                    }
                } else {
                    // TODO: cluster mode support grouped writer
                    throw new UnsupportedOperationException();
                }
                this.currentPartition = partition;
                super.open(this.taskNumber, this.numTasks);
            } catch (IOException e) {
                LOG.error("change partition error, current partition: " + currentPartition);
                throw e;
            }
        }
        super.writeRecord(record);
    }

    @Override
    protected void checkPartition(String partitionSpec) {
        Preconditions.checkArgument(this.isPartitioned,
                "Table " + tableName + " is not partitioned");
        OdpsUtils.checkPartition(partitionSpec, getTableSchema());
        this.staticPartition = partitionSpec;
        this.currentPartition = "";
    }
}
