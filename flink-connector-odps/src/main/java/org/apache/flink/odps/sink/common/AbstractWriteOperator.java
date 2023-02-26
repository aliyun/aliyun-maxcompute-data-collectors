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

import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.lang.reflect.Field;

public abstract class AbstractWriteOperator<I> extends ProcessOperator<I, Object>
        implements OperatorEventHandler, BoundedOneInput {

    private final AbstractWriteFunction<I> function;
    protected MailboxExecutor mainMailboxExecutor;

    public AbstractWriteOperator(AbstractWriteFunction<I> function) {
        super(function);
        this.function = function;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Object>> output) {
        super.setup(containingTask, config, output);
        // TODO: set up mainMailboxExecutor
        try {
            Field unsafeField = StreamTask.class.getDeclaredField("mainMailboxExecutor");
            unsafeField.setAccessible(true);
            mainMailboxExecutor = (MailboxExecutor) unsafeField.get(containingTask);
        } catch (Throwable cause) {
            mainMailboxExecutor = null;
        }
        this.function.setMailboxExecutor(mainMailboxExecutor);
    }

    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        this.function.setOperatorEventGateway(operatorEventGateway);
    }

    @Override
    public void endInput() {
        this.function.endInput();
    }

    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        this.function.handleOperatorEvent(evt);
    }
}
