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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.odps.sink.table;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.DataFormat;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.table.configuration.DynamicPartitionOptions;
import com.aliyun.odps.table.distribution.Distribution;
import com.aliyun.odps.table.distribution.UnspecifiedDistribution;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.order.SortOrder;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.table.write.TableUpsertSession;
import com.aliyun.odps.table.write.TableWriteCapabilities;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

public abstract class TableUpsertSessionBase implements TableUpsertSession {

    protected transient EnvironmentSettings settings;

    protected PartitionSpec targetPartitionSpec;

    protected boolean overwrite;

    protected DynamicPartitionOptions dynamicPartitionOptions;

    protected ArrowOptions arrowOptions;

    protected TableWriteCapabilities writeCapabilities;

    protected String sessionId;

    protected TableIdentifier identifier;

    protected SessionStatus sessionStatus;

    protected long expirationTime;

    protected String errorMessage;

    protected DataSchema requiredSchema;

    protected SortOrder[] requiredSortOrders;

    protected Distribution requiredDistribution;

    protected long maxBlockNumber;

    protected Set<DataFormat> supportDataFormats;

    public TableUpsertSessionBase(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  boolean overwrite,
                                  DynamicPartitionOptions dynamicPartitionOptions,
                                  ArrowOptions arrowOptions,
                                  TableWriteCapabilities capabilities,
                                  EnvironmentSettings settings) throws IOException {
        Preconditions.checkNotNull(identifier, "Table identifier", "required");
        Preconditions.checkNotNull(settings, "Environment settings", "required");
        this.settings = settings;
        this.identifier = identifier;
        this.overwrite = overwrite;
        sanitize(partitionSpec, dynamicPartitionOptions, arrowOptions, capabilities);
        initSession();
    }

    public TableUpsertSessionBase(TableIdentifier identifier,
                                  PartitionSpec partitionSpec,
                                  String sessionId,
                                  EnvironmentSettings settings) throws IOException {
        Preconditions.checkNotNull(identifier, "Table identifier", "required");
        Preconditions.checkNotNull(settings, "Environment settings", "required");

        this.sessionId = sessionId;
        this.identifier = identifier;
        this.settings = settings;
        this.targetPartitionSpec = partitionSpec == null ? new PartitionSpec() : partitionSpec;
        reloadSession();
    }

    protected abstract void initSession() throws IOException;

    protected abstract void reloadSession() throws IOException;

    private void sanitize(PartitionSpec partitionSpec,
                          DynamicPartitionOptions dynamicPartitionOptions,
                          ArrowOptions arrowOptions,
                          TableWriteCapabilities writeCapabilities) {
        this.targetPartitionSpec = partitionSpec == null ?
                new PartitionSpec() : partitionSpec;
        this.dynamicPartitionOptions = dynamicPartitionOptions == null ?
                DynamicPartitionOptions.createDefault() : dynamicPartitionOptions;
        this.arrowOptions = arrowOptions == null ?
                ArrowOptions.createDefault() : arrowOptions;
        this.writeCapabilities = writeCapabilities == null ?
                TableWriteCapabilities.createDefault() : writeCapabilities;
    }

    @Override
    public String getId() {
        if (this.sessionId != null) {
            return this.sessionId;
        } else {
            throw new IllegalStateException(
                    "The table sink has not been initialized yet");
        }
    }

    @Override
    public DataSchema requiredSchema() {
        if (this.requiredSchema != null) {
            return this.requiredSchema;
        } else {
            throw new IllegalStateException(
                    "The table sink has not been initialized yet");
        }
    }

    @Override
    public Optional<Long> maxBlockNumber() {
        return maxBlockNumber > 0 ?
                Optional.of(maxBlockNumber) : Optional.empty();
    }

    @Override
    public Distribution requiredDistribution() {
        return requiredDistribution != null ?
                requiredDistribution : new UnspecifiedDistribution();
    }

    @Override
    public SortOrder[] requiredOrdering() {
        return requiredSortOrders != null ?
                requiredSortOrders : new SortOrder[]{};
    }

    @Override
    public TableIdentifier getTableIdentifier() {
        return identifier;
    }

    @Override
    public SessionStatus getStatus() {
        return sessionStatus;
    }
}
