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

package org.apache.flink.odps.sink.table;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.*;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.TableWriteCapabilities;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TableUpsertSessionImpl extends TableUpsertSessionBase {

    private static final Logger LOG = LoggerFactory.getLogger(TableUpsertSessionImpl.class);

    protected transient TableTunnel.UpsertSession session;
    protected transient RetryStrategy commitRetry;
    protected boolean isCommitted;
    protected boolean isClosed;

    public TableUpsertSessionImpl(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  boolean overwrite,
                                  DynamicPartitionOptions dynamicPartitionOptions,
                                  ArrowOptions arrowOptions,
                                  TableWriteCapabilities capabilities,
                                  EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, overwrite,
                dynamicPartitionOptions, arrowOptions, capabilities, settings);
        this.isCommitted = false;
        this.isClosed = false;
        this.commitRetry =
                new RetryStrategy(3, 5, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
    }

    public TableUpsertSessionImpl(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  String sessionId,
                                  EnvironmentSettings settings) throws IOException {
        super(identifier, partitionSpec, sessionId, settings);
        this.isCommitted = false;
        this.isClosed = false;
        this.commitRetry =
                new RetryStrategy(3, 5, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
    }

    @Override
    protected void initSession() throws IOException {
        try {
            TableTunnel tunnel = TableUtils.getTableTunnel(settings);
            this.session = tunnel.buildUpsertSession(identifier.getProject(), identifier.getTable())
                    .setPartitionSpec(targetPartitionSpec)
                    .build();
            this.sessionId = session.getId();
            this.sessionStatus = SessionStatus.valueOf(session.getStatus().toUpperCase());
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void reloadSession() throws IOException {
        try {
            TableTunnel tunnel = TableUtils.getTableTunnel(settings);
            this.session = tunnel.buildUpsertSession(identifier.getProject(), identifier.getTable())
                            .setPartitionSpec(targetPartitionSpec)
                            .setUpsertId(getId())
                            .build();
            this.sessionStatus = SessionStatus.valueOf(session.getStatus().toUpperCase());
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    public UpsertWriter<ArrayRecord> createUpsertWriter() throws IOException {
        Preconditions.checkNotNull(session, "Upsert session", "required");
        return new TableUpsertWriterImpl(session);
    }

    public void commit() throws IOException {
        Preconditions.checkNotNull(session, "Upsert session", "required");
        if (isCommitted) {
            return;
        }
        commitRetry.reset();
        while (true) {
            try {
                session.commit(false);
                break;
            } catch (Exception exception) {
                LOG.error(String.format("Commit error, retry times = %d",
                        commitRetry.getAttempts()), exception);
                try {
                    commitRetry.onFailure(exception);
                } catch (RetryExceedLimitException | InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
        isCommitted = true;
    }

    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        session.close();
        isClosed = true;
    }

    @Override
    public boolean supportsDataFormat(DataFormat dataFormat) {
        return dataFormat.getType().equals(DataFormat.Type.RECORD);
    }

    @Override
    public SessionType getType() {
        return SessionType.UPSERT;
    }
}
