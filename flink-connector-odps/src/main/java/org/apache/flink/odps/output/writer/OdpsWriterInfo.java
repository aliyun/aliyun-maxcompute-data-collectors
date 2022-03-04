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

import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;

import java.io.Serializable;

public class OdpsWriterInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String partitionSpec;
    private WriteSessionInfo writeSessionInfo;
    private long sessionCreateTime;
    private int taskNumber;
    private int writeShares;
    private int currentBlockId;
    private boolean isClosed;

    public OdpsWriterInfo(String partitionSpec) {
        this.partitionSpec = partitionSpec;
        this.isClosed = false;
    }

    public OdpsWriterInfo(String partitionSpec, WriteSessionInfo writeSessionInfo, long sessionCreateTime) {
        this.partitionSpec = partitionSpec;
        this.sessionCreateTime = sessionCreateTime;
        this.writeSessionInfo = writeSessionInfo;
        this.isClosed = false;
    }

    public void setTaskNumber(int taskNumber) {
        this.taskNumber = taskNumber;
    }

    public void setClosed(boolean closed) {
        this.isClosed = closed;
    }

    public void setCurrentBlockId(int currentBlockId) {
        this.currentBlockId = currentBlockId;
    }

    public void setWriteShares(int writeShares) {
        this.writeShares = writeShares;
    }

    public int getCurrentBlockId() {
        return currentBlockId;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    public WriteSessionInfo getWriteSessionInfo() {
        return writeSessionInfo;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public int getWriteShares() {
        return writeShares;
    }

    public long getSessionCreateTime() {
        return sessionCreateTime;
    }
}
