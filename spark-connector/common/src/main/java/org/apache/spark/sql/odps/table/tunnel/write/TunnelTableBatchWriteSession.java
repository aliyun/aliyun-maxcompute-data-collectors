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

package org.apache.spark.sql.odps.table.tunnel.write;

import com.aliyun.odps.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.configuration.WriterOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.BatchWriter;
import com.aliyun.odps.table.write.WriterAttemptId;
import com.aliyun.odps.table.write.WriterCommitMessage;
import com.aliyun.odps.table.write.impl.batch.TableBatchWriteSessionBase;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.odps.table.utils.TableUtils;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class TunnelTableBatchWriteSession extends TableBatchWriteSessionBase {

    protected transient TableTunnel.UploadSession session;

    public TunnelTableBatchWriteSession(TableIdentifier identifier,
                                        PartitionSpec partitionSpec,
                                        boolean overwrite,
                                        DynamicPartitionOptions dynamicPartitionOptions,
                                        ArrowOptions arrowOptions,
                                        EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, overwrite, dynamicPartitionOptions, arrowOptions, null, settings);
    }

    public TunnelTableBatchWriteSession(TableIdentifier identifier,
                                        String sessionId,
                                        EnvironmentSettings settings) throws IOException {
        super(identifier, sessionId, settings);
    }

    @Override
    protected void initSession() throws IOException {
        Preconditions.checkArgument(dynamicPartitionOptions.getInvalidStrategy().equals
                        (DynamicPartitionOptions.InvalidStrategy.EXCEPTION),
                "Strategy not supported:" + dynamicPartitionOptions.getInvalidStrategy());

        ExecutionEnvironment env = ExecutionEnvironment.create(settings);
        Odps odps = env.createOdpsClient();
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(env.getTunnelEndpoint(identifier.getProject()));
        // TODO: support schema
        Table table = odps.tables().get(identifier.getProject(),
                identifier.getTable());
        try {
            if (table.isPartitioned()) {
                // check dynamic partition
                List<Column> partitionColumns = table.getSchema().getPartitionColumns();
                if (partitionColumns.size() != targetPartitionSpec.keys().size()) {
                    throw new InvalidParameterException("Dynamic partition is not supported");
                } else {
                    TableUtils.validatePartitionSpec(targetPartitionSpec, partitionColumns.stream()
                            .map(Column::getName).collect(Collectors.toList()));
                }

                // TODO: support schema
                if (!table.hasPartition(targetPartitionSpec)) {
                    createPartition(identifier.getProject(), identifier.getTable(),
                            targetPartitionSpec, odps);
                }
            }
        } catch (OdpsException e) {
            throw new IOException(e);
        }

        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                // TODO: support schema
                if (targetPartitionSpec.isEmpty()) {
                    session = tunnel.createUploadSession(identifier.getProject(),
                            identifier.getTable(),
                            overwrite);
                } else {
                    session = tunnel.createUploadSession(identifier.getProject(),
                            identifier.getTable(),
                            targetPartitionSpec,
                            overwrite);
                }
                TableSchema tableSchema = session.getSchema();
                this.requiredSchema = DataSchema.newBuilder()
                        .columns(tableSchema.getColumns())
                        .build();
                this.sessionId = session.getId();
                break;
            } catch (TunnelException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
    }

    @Override
    protected String reloadSession() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) throws IOException {
        List<Long> blocks = new ArrayList<>();
        for (int i = 0; i < messages.length; i++) {
            if (messages[i] == null) {
                continue;
            }
            TunnelCommitMessage tunnelCommitMessage = (TunnelCommitMessage) messages[i];
            blocks.add(tunnelCommitMessage.getBlockId());
        }
        try {
            // TODO: check session
            session.commit(blocks.toArray(new Long[0]));
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void cleanup() {
        // Do noting
    }

    @Override
    public BatchWriter<ArrayRecord> createRecordWriter(long writerId,
                                                       WriterAttemptId attemptId,
                                                       WriterOptions options) throws IOException {
        return new TunnelRecordWriter(sessionId, identifier, requiredSchema, writerId, options, targetPartitionSpec);
    }

    public BatchWriter<VectorSchemaRoot> createArrowWriter(long writerId,
                                                           WriterAttemptId attemptId,
                                                           WriterOptions options) throws IOException {
        return new TunnelArrowBatchWriter(sessionId, identifier, requiredSchema, writerId, options, targetPartitionSpec, arrowOptions);
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        if (dataFormat.getType().equals(DataFormat.Type.RECORD)) {
            return true;
        }
        if (dataFormat.getType().equals(DataFormat.Type.ARROW)) {
            return requiredSchema.getColumns().stream()
                    .map(Column::getType)
                    .noneMatch(odpsType -> odpsType.equals(OdpsType.STRUCT)
                            || odpsType.equals(OdpsType.ARRAY)
                            || odpsType.equals(OdpsType.MAP));
        }
        return false;
    }

    public static void createPartition(String project, String table,
                                       PartitionSpec partitionSpec, Odps odps) throws IOException {
        int retry = 0;
        long sleep = 2000;
        while (true) {
            try {
                // TODO: support schema
                odps.tables().get(project, table).createPartition(partitionSpec, true);
                break;
            } catch (OdpsException e) {
                retry++;
                if (retry > 5) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                sleep = sleep * 2;
            }
        }
    }
}
