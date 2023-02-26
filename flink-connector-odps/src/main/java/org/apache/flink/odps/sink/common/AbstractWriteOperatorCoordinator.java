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

package org.apache.flink.odps.sink.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.adapter.OperatorCoordinatorAdapter;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.utils.NonThrownExecutor;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public abstract class AbstractWriteOperatorCoordinator implements OperatorCoordinatorAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractWriteOperatorCoordinator.class);

    protected final String operatorName;
    protected final Configuration conf;
    protected final OperatorCoordinator.Context context;
    protected transient OperatorCoordinator.SubtaskGateway[] gateways;
    protected transient SinkTaskEvent[] eventBuffer;
    protected final int parallelism;
    protected NonThrownExecutor executor;

    /**
     * Constructs a StreamingSinkOperatorCoordinator.
     *
     * @param conf    The config options
     * @param context The coordinator context
     */
    public AbstractWriteOperatorCoordinator(
            String operatorName,
            Configuration conf,
            OperatorCoordinator.Context context) {
        this.operatorName = operatorName;
        this.conf = conf;
        this.context = context;
        this.parallelism = context.currentParallelism();
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting odps write operator {}.", operatorName);

        Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
        this.eventBuffer = new SinkTaskEvent[this.parallelism];
        this.gateways = new OperatorCoordinator.SubtaskGateway[this.parallelism];
        // start the executor
        this.executor = NonThrownExecutor.builder(LOG)
                .exceptionHook((errMsg, t) -> this.context.failJob(new FlinkOdpsException(errMsg, t)))
                .waitForTasksFinish(true).build();
        initEnv();
    }

    @Override
    public void close() throws Exception {
        if (executor != null) {
            executor.close();
        }
        this.eventBuffer = null;
        LOG.info("Odps write coordinator {} closed.", operatorName);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) throws Exception {
        executor.execute(
                () -> {
                    try {
                        result.complete(new byte[0]);
                    } catch (Throwable throwable) {
                        ExceptionUtils.rethrowIfFatalErrorOrOOM(throwable);
                        // when a checkpoint fails, throws directly.
                        result.completeExceptionally(
                                new CompletionException(
                                        String.format("Failed to checkpoint Session %s for source %s",
                                                checkpointId, this.getClass().getSimpleName()), throwable));
                    }
                }, "taking checkpoint %d", checkpointId
        );
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // no operation
    }

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        // no operation
    }

    @Override
    public void handleEventFromOperator(int i, OperatorEvent operatorEvent) {
        Preconditions.checkState(operatorEvent instanceof SinkTaskEvent,
                "The coordinator can only handle SinkTaskEvent");
        SinkTaskEvent event = (SinkTaskEvent) operatorEvent;

        if (event.isEndInput()) {
            executor.executeSync(() -> handleEndInputEvent(event), "handle end input event");
        } else {
            executor.execute(
                    () -> {
                        if (event.isBootstrap()) {
                            handleBootstrapEvent(event);
                        } else {
                            handleCommitEvent(event);
                        }
                    }, "handle sink event"
            );
        }
    }

    @Override
    public void subtaskFailed(int i, @Nullable Throwable throwable) {
        // reset the event
        this.eventBuffer[i] = null;
        LOG.warn("Reset the event for task [" + i + "]", throwable);
    }

    @Override
    public void subtaskReset(int i, long l) {
        // no operation
    }

    @Override
    public void subtaskReady(int i, SubtaskGateway subtaskGateway) {
        this.gateways[i] = subtaskGateway;
    }

    abstract protected void initEnv();

    abstract protected void handleBootstrapEvent(SinkTaskEvent event) throws IOException;

    abstract protected void handleCommitEvent(SinkTaskEvent event) throws IOException;

    abstract protected void handleEndInputEvent(SinkTaskEvent event) throws IOException;
}
