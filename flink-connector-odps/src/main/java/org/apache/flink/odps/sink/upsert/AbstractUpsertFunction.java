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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.sink.common.AbstractWriteFunction;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractUpsertFunction<I>
    extends AbstractWriteFunction<I>
    implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractUpsertFunction.class);

    protected final Configuration config;
    protected int taskID;

    protected List<WriterStatus> writerStatuses;
    protected transient OperatorEventGateway eventGateway;
    protected transient boolean inputEnded;
    protected transient ConcurrentHashMap<String, String> sessionRequest;

    public AbstractUpsertFunction(Configuration config) {
        this.config = config;
    }

    @Override
    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        this.eventGateway = operatorEventGateway;
    }

    @Override
    public void endInput() {
        this.inputEnded = true;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.taskID = getRuntimeContext().getIndexOfThisSubtask();
        this.writerStatuses = new ArrayList<>();
        this.sessionRequest = new ConcurrentHashMap<>();
        sendBootstrapEvent();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (inputEnded) {
            return;
        }
        snapshotState(context.getCheckpointId());
    }

    protected abstract void sendBootstrapEvent();

    protected abstract void snapshotState(long checkpointId) throws IOException;

}