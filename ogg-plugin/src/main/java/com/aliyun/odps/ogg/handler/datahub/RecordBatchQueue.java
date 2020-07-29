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

package com.aliyun.odps.ogg.handler.datahub;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RecordBatchQueue {
    private volatile RecordBatch front = null;
    private final LinkedBlockingQueue<RecordBatch> bufferQueue;

    public RecordBatchQueue(int capacity) {
        this.bufferQueue = new LinkedBlockingQueue<>(capacity);
    }

    public int size() {
        return bufferQueue.size() + (front == null ? 0 : 1);
    }

    public boolean isEmpty() {
        return front == null && bufferQueue.isEmpty();
    }

    public RecordBatch peek() {
        return front == null ? bufferQueue.peek() : front;
    }

    public boolean offer(RecordBatch recordBatch, long timeout, TimeUnit unit) throws InterruptedException {
        return bufferQueue.offer(recordBatch, timeout, unit);
    }

    public void pop() {
        if (front == null) {
            bufferQueue.poll();
        } else {
            front = null;
        }
    }

    public boolean waitEmpty(long timeout, TimeUnit unit) throws InterruptedException {
        if (front == null) {
            front = bufferQueue.poll(timeout, unit);
        }

        return isEmpty();
    }
}
