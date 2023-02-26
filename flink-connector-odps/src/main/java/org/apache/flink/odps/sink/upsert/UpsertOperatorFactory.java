/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.DataSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.AbstractWriteOperator;
import org.apache.flink.odps.sink.common.AbstractWriteOperatorFactory;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class UpsertOperatorFactory extends AbstractWriteOperatorFactory<RowData> {

    private final OdpsUpsertOperatorFactoryBuilder builder;

    public UpsertOperatorFactory(Configuration conf,
                                 AbstractWriteOperator<RowData> operator,
                                 OdpsUpsertOperatorFactoryBuilder builder) {
        super(conf, operator);
        this.builder = builder;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String operatorName, OperatorID operatorID) {
        return new UpsertOperatorCoordinator.Provider(
                operatorName,
                operatorID,
                builder);
    }

    /**
     * Builder to build {@link UpsertOperator}.
     */
    public static class OdpsUpsertOperatorFactoryBuilder implements Serializable {
        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String partition;
        private DataSchema dataSchema;
        private OdpsWriteOptions writeOptions;
        private boolean isDynamicPartition = false;
        private boolean supportPartitionGrouping = false;
        private PartitionAssigner<RowData> partitionAssigner;
        private Configuration conf;

        public OdpsUpsertOperatorFactoryBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OdpsUpsertOperatorFactoryBuilder(OdpsConf odpsConf,
                                                String projectName,
                                                String tableName) {
            this(odpsConf, projectName, tableName, null);
        }

        public OdpsUpsertOperatorFactoryBuilder(OdpsConf odpsConf,
                                                String projectName,
                                                String tableName,
                                                OdpsWriteOptions writeOptions) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
            this.writeOptions = writeOptions;
        }

        public OdpsUpsertOperatorFactoryBuilder setConf(Configuration conf) {
            this.conf = conf;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setOdpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setTableSchema(TableSchema schema) {
            List<Column> schemaColumns = new ArrayList<>();
            schemaColumns.addAll(schema.getColumns());
            schemaColumns.addAll(schema.getPartitionColumns());
            this.dataSchema = DataSchema.newBuilder()
                    .columns(schemaColumns)
                    .partitionBy(schema.getPartitionColumns()
                            .stream()
                            .map(Column::getName)
                            .collect(Collectors.toList()))
                    .build();
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setWriteOptions(OdpsWriteOptions writeOptions) {
            this.writeOptions = writeOptions;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setDynamicPartition(boolean dynamicPartition) {
            this.isDynamicPartition = dynamicPartition;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setSupportPartitionGrouping(boolean supportPartitionGrouping) {
            this.supportPartitionGrouping = supportPartitionGrouping;
            return this;
        }

        public OdpsUpsertOperatorFactoryBuilder setPartitionAssigner(PartitionAssigner<RowData> partitionAssigner) {
            this.partitionAssigner = partitionAssigner;
            return this;
        }

        public OdpsConf getOdpsConf() {
            return odpsConf;
        }

        public String getPartition() {
            return partition;
        }

        public String getTableName() {
            return tableName;
        }

        public String getProjectName() {
            return projectName;
        }

        public Configuration getConf() {
            return conf;
        }

        public OdpsWriteOptions getWriteOptions() {
            return writeOptions;
        }

        public PartitionAssigner<RowData> getPartitionAssigner() {
            return partitionAssigner;
        }

        public DataSchema getDataSchema() {
            return dataSchema;
        }

        public boolean isDynamicPartition() {
            return isDynamicPartition;
        }

        public boolean isSupportPartitionGrouping() {
            return supportPartitionGrouping;
        }

        public UpsertOperatorFactory build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");
            return new UpsertOperatorFactory(conf,
                    new UpsertOperator(this),
                    this);
        }
    }
}
