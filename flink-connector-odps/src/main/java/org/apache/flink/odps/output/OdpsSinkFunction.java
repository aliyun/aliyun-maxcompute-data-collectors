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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsStreamWrite;
import org.apache.flink.odps.output.writer.OdpsWriteFactory;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class shared between the Java and Scala API of Flink
 */
@Public
public class OdpsSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(OdpsSinkFunction.class);

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private final String partition;
    private final boolean isDynamicPartition;
    private final boolean supportsGrouping;

    private transient OdpsStreamWrite<T> odpsStreamWrite;
    private final OdpsWriteOptions writeOptions;
    private final PartitionAssigner<T> partitionAssigner;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;
    private transient volatile boolean closed;

    public OdpsSinkFunction(
            String projectName,
            String tableName) {
        this((OdpsConf) null, projectName, tableName, false, false);
    }

    public OdpsSinkFunction(
            String projectName,
            String tableName,
            String partition) {
        this((OdpsConf) null, projectName, tableName, partition, false, false);
    }

    public OdpsSinkFunction(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            boolean isDynamicPartition,
            boolean supportsGrouping) {
        this(odpsConf, projectName, tableName, "", isDynamicPartition, supportsGrouping);
    }

    public OdpsSinkFunction(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isDynamicPartition,
            boolean supportsGrouping) {
        this(odpsConf, projectName, tableName, partition, isDynamicPartition, supportsGrouping,
                null, null);
    }

    public OdpsSinkFunction(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<T> partitionAssigner) {
        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");
        if (this.odpsConf.isClusterMode()) {
            throw new IllegalStateException("Odps sink function cannot support overwrite in cluster mode.");
        }
        this.projectName = Preconditions.checkNotNull(projectName, "project cannot be null");
        this.tableName = Preconditions.checkNotNull(tableName, "table cannot be null");
        this.partition = partition;
        this.isDynamicPartition = isDynamicPartition;
        this.supportsGrouping = supportsGrouping;
        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;
        this.partitionAssigner = partitionAssigner;
        LOG.info("Create odps sink, table:{}.{}, partition:{},isDynamicPartition:{},supportsGrouping:{},writeOptions:{}",
                projectName, tableName, partition, isDynamicPartition, supportsGrouping, writeOptions);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        synchronized (this) {
            if (!closed) {
                try {
                    odpsStreamWrite.flush();
                } catch (Exception e) {
                    flushException = e;
                }
            }
        }
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        this.odpsStreamWrite = OdpsWriteFactory.createOdpsStreamWrite(
                odpsConf,
                projectName,
                tableName,
                partition,
                isDynamicPartition,
                supportsGrouping,
                writeOptions,
                partitionAssigner);
        RuntimeContext ctx = getRuntimeContext();
        this.closed = false;
        odpsStreamWrite.initWriteSession();
        odpsStreamWrite.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        if (writeOptions.getBufferFlushIntervalMillis() > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("odps-sink-function"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (this) {
                                    if (!closed) {
                                        try {
                                            odpsStreamWrite.flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            writeOptions.getBufferFlushIntervalMillis(),
                            writeOptions.getBufferFlushIntervalMillis(),
                            TimeUnit.MILLISECONDS);
        }
        LOG.info("Open odps sink function");
    }

    @Override
    public final void invoke(
            T value, Context context) throws Exception {
        odpsStreamWrite.updateWriteContext(context);
        synchronized (this) {
            checkFlushException();
            try {
                odpsStreamWrite.writeRecord(value);
            } catch (Exception e) {
                throw new IOException("Writing records to Odps failed.", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        synchronized (this) {
            if (!closed) {
                closed = true;
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(false);
                    scheduler.shutdown();
                }
                if (odpsStreamWrite != null) {
                    try {
                        odpsStreamWrite.flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
            checkFlushException();
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Odps failed.", flushException);
        }
    }

    /**
     * Builder to build {@link OdpsSinkFunction}.
     */
    public static class OdpsSinkBuilder<T> {
        private OdpsConf odpsConf;
        private String projectName;
        private String tableName;
        private String partition;
        private boolean isDynamicPartition;
        private boolean supportPartitionGrouping;
        private OdpsWriteOptions writeOptions;
        private PartitionAssigner<T> partitionAssigner;

        public OdpsSinkBuilder(String projectName, String tableName) {
            this(null, projectName, tableName);
        }

        public OdpsSinkBuilder(OdpsConf odpsConf,
                               String projectName,
                               String tableName) {
            this(odpsConf, projectName, tableName,null);
        }

        public OdpsSinkBuilder(OdpsConf odpsConf,
                               String projectName,
                               String tableName,
                               OdpsWriteOptions writeOptions) {
            this.odpsConf = odpsConf;
            this.projectName = projectName;
            this.tableName = tableName;
            this.writeOptions = writeOptions;
        }

        public OdpsSinkBuilder<T> setOdpsConf(OdpsConf odpsConf) {
            this.odpsConf = odpsConf;
            return this;
        }

        public OdpsSinkBuilder<T> setProjectName(String projectName) {
            this.projectName = projectName;
            return this;
        }

        public OdpsSinkBuilder<T> setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public OdpsSinkBuilder<T> setPartition(String partition) {
            this.partition = partition;
            return this;
        }

        public OdpsSinkBuilder<T> setDynamicPartition(boolean dynamicPartition) {
            this.isDynamicPartition = dynamicPartition;
            return this;
        }

        public OdpsSinkBuilder<T> setSupportPartitionGrouping(boolean supportPartitionGrouping) {
            this.supportPartitionGrouping = supportPartitionGrouping;
            return this;
        }

        public OdpsSinkBuilder<T> setWriteOptions(OdpsWriteOptions writeOptions) {
            this.writeOptions = writeOptions;
            return this;
        }

        public OdpsSinkBuilder<T> setPartitionAssigner(PartitionAssigner<T> partitionAssigner) {
            this.partitionAssigner = partitionAssigner;
            return this;
        }

        public OdpsSinkFunction<T> build() {
            checkNotNull(projectName, "projectName should not be null");
            checkNotNull(tableName, "tableName should not be null");
            return new OdpsSinkFunction<>(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    isDynamicPartition,
                    supportPartitionGrouping,
                    writeOptions,
                    partitionAssigner);
        }
    }
}
