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

package org.apache.flink.odps.output;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.FinalizeOnMaster;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.writer.OdpsTableWrite;
import org.apache.flink.odps.output.writer.OdpsWriteFactory;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class OdpsOutputFormat<T> extends RichOutputFormat<T> implements InitializeOnMaster, FinalizeOnMaster {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OdpsOutputFormat.class);

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private final String partition;

    private final boolean isOverwrite;
    private final boolean isDynamicPartition;
    private final boolean supportsGrouping;

    private final boolean isLocal;

    private final OdpsTableWrite<T> odpsWrite;
    private final OdpsWriteOptions writeOptions;

    public OdpsOutputFormat(String projectName,
                            String tableName,
                            boolean isOverwrite) {
        this((OdpsConf) null, projectName, tableName, isOverwrite, false, false);
    }

    public OdpsOutputFormat(String projectName,
                            String tableName,
                            boolean isOverwrite,
                            boolean isDynamicPartition,
                            boolean supportsGrouping) {
        this((OdpsConf) null, projectName, tableName, isOverwrite, isDynamicPartition, supportsGrouping);
    }

    public OdpsOutputFormat(OdpsConf odpsConf,
                            String projectName,
                            String tableName,
                            boolean isOverwrite,
                            boolean isDynamicPartition,
                            boolean supportsGrouping) {
        this(odpsConf, projectName, tableName, "", isOverwrite, isDynamicPartition, supportsGrouping, null);
    }

    public OdpsOutputFormat(OdpsConf odpsConf,
                            String projectName,
                            String tableName,
                            String partition,
                            boolean isOverwrite) {
        this(odpsConf, projectName, tableName, partition, isOverwrite, false, false, null);
    }

    public OdpsOutputFormat(String projectName,
                            String tableName,
                            String partition,
                            boolean isOverwrite) {
        this((OdpsConf) null, projectName, tableName, partition, isOverwrite, false, false, null);
    }

    public OdpsOutputFormat(String projectName,
                            String tableName,
                            String partition,
                            boolean isOverwrite,
                            boolean isDynamicPartition,
                            boolean supportsGrouping) {
        this((OdpsConf) null, projectName, tableName, partition, isOverwrite, isDynamicPartition, supportsGrouping, null);
    }

    public OdpsOutputFormat(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isOverwrite,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions options) {

        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");

        this.projectName = Preconditions.checkNotNull(projectName, "project cannot be null");
        this.tableName = Preconditions.checkNotNull(tableName, "table name cannot be null");
        this.partition = partition;
        this.isOverwrite = isOverwrite;
        this.isDynamicPartition = isDynamicPartition;
        this.supportsGrouping = supportsGrouping;
        this.isLocal = !this.odpsConf.isClusterMode();

        if (isLocal && (isDynamicPartition || supportsGrouping)) {
            throw new UnsupportedOperationException("Cannot support dynamic partition in local mode by OdpsOutputFormat");
        }

        this.writeOptions = options == null ?
                OdpsWriteOptions.builder().build() : options;

        LOG.info("Create odps output, table:{}.{},partition:{},isOverwrite:{},isDynamicPartition:{},supportsGrouping:{},writeOptions:{}",
                projectName, tableName, partition, isOverwrite, isDynamicPartition, supportsGrouping, writeOptions);

        this.odpsWrite = OdpsWriteFactory.createOdpsFileWrite(
                odpsConf,
                projectName,
                tableName,
                partition,
                isOverwrite,
                isDynamicPartition,
                supportsGrouping,
                this.writeOptions,
                null);
        try {
            this.odpsWrite.initWriteSession();
        } catch (IOException e){
            throw new FlinkOdpsException("Init odps write session failed!", e);
        }
    }

    public OdpsOutputFormat<T> setOdpsConf(OdpsConf odpsConf) {
        return setOdpsConf(odpsConf, false);
    }

    public OdpsOutputFormat<T> setOdpsConf(OdpsConf odpsConf, boolean forceLocalRun) {
        if (odpsConf != null) {
            odpsConf.setClusterMode(!forceLocalRun);
            OutputFormatBuilder<T> builder = new OutputFormatBuilder<>(odpsConf, projectName, tableName);
            builder.setDynamicPartition(isDynamicPartition);
            builder.setOverwrite(isOverwrite);
            builder.setSupportPartitionGrouping(supportsGrouping);
            builder.setPartition(partition);
            builder.setWriteOptions(writeOptions);
            return builder.build();
        }
        return this;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void initializeGlobal(int parallelism) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            LOG.info("open odps file writer number {}", taskNumber);
            odpsWrite.open(taskNumber, numTasks);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeRecord(T record) throws IOException {
        odpsWrite.writeRecord(record);
    }

    @Override
    public void close() throws IOException {
        odpsWrite.close();
    }

    @Override
    public void finalizeGlobal(int parallelism) throws IOException {
        LOG.info("Commit odps write session!");
        odpsWrite.commitWriteSession();
    }

    /**
     * Builder to build {@link OdpsOutputFormat}.
     */
    public static class OutputFormatBuilder<T> {

        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String partition;
        private boolean isOverwrite;
        private boolean isDynamicPartition;
        private boolean supportPartitionGrouping;
        private OdpsWriteOptions writeOptions;

        public OutputFormatBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OutputFormatBuilder(OdpsConf odpsConf, String projectName, String tableName) {
            this(odpsConf, projectName, tableName, false);
        }

        public OutputFormatBuilder(OdpsConf odpsConf, String projectName, String tableName, boolean isOverwrite) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
            this.isOverwrite = isOverwrite;
        }

        public OutputFormatBuilder<T> setOdpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public OutputFormatBuilder<T> setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public OutputFormatBuilder<T> setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public OutputFormatBuilder<T> setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public OutputFormatBuilder<T> setOverwrite(boolean overwrite) {
            this.isOverwrite = overwrite;
            return this;
        }

        public OutputFormatBuilder<T> setDynamicPartition(boolean dynamicPartition) {
            this.isDynamicPartition = dynamicPartition;
            return this;
        }

        public OutputFormatBuilder<T> setSupportPartitionGrouping(boolean supportPartitionGrouping) {
            this.supportPartitionGrouping = supportPartitionGrouping;
            return this;
        }

        public OutputFormatBuilder<T> setWriteOptions(OdpsWriteOptions writeOptions) {
            this.writeOptions = writeOptions;
            return this;
        }

        public OdpsOutputFormat<T> build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");
            return new OdpsOutputFormat<T>(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    isOverwrite,
                    isDynamicPartition,
                    supportPartitionGrouping,
                    writeOptions
            );
        }
    }
}
