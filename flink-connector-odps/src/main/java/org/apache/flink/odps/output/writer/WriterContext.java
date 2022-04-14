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

package org.apache.flink.odps.output.writer;

import org.apache.flink.odps.output.stream.PartitionAssigner;

import javax.annotation.Nullable;
import java.io.Serializable;

public class WriterContext implements PartitionAssigner.Context, Serializable {

    @Nullable
    private Long elementTimestamp;

    private long currentWatermark;

    private long currentProcessingTime;

    private String staticPartition;

    public WriterContext(String staticPartition) {
        this.elementTimestamp = null;
        this.currentWatermark = Long.MIN_VALUE;
        this.currentProcessingTime = Long.MIN_VALUE;
        this.staticPartition = staticPartition;
    }

    public void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
        this.elementTimestamp = elementTimestamp;
        this.currentWatermark = watermark;
        this.currentProcessingTime = processingTime;
    }

    @Override
    public long currentProcessingTime() {
        return currentProcessingTime;
    }

    @Override
    public long currentWatermark() {
        return currentWatermark;
    }

    @Override
    public String getStaticPartition() {
        return staticPartition;
    }

    @Override
    @Nullable
    public Long timestamp() {
        return elementTimestamp;
    }

}
