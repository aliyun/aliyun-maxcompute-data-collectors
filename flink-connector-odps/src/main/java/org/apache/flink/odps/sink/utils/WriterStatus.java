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

package org.apache.flink.odps.sink.utils;

import java.io.Serializable;

public class WriterStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private String sessionId;

    private String partitionSpec;

    private String writerMessage;

    private long totalRecords;

    public WriterStatus() {
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public String getWriterMessage() {
        return writerMessage;
    }

    public void setPartitionSpec(String partitionSpec) {
        this.partitionSpec = partitionSpec;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void setWriterMessage(String writerMessage) {
        this.writerMessage = writerMessage;
    }
}