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

package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.TableProvider;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.aliyun.odps.cupid.table.v1.writer.WriteCapabilities;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.Map;

public class TunnelProvider implements TableProvider {

    @Override
    public String getShortName() {
        return "tunnel";
    }

    @Override
    public ReadCapabilities getReadCapabilities() {
        return new ReadCapabilities(false, false, false);
    }

    @Override
    public TableReadSession createReadSession(String project,
                                              String table,
                                              TableSchema tableSchema,
                                              RequiredSchema readDataColumns,
                                              List<Map<String, String>> readPartitions,
                                              Options configs) {
        return new TunnelReadSession(project, table, tableSchema, readDataColumns, readPartitions, configs);
    }

    @Override
    public SplitReader<ArrayRecord> createRecordReader(InputSplit inputSplit) {
        return new TunnelReader((TunnelInputSplit) inputSplit);
    }

    @Override
    public SplitReader<ColDataBatch> createColDataReader(InputSplit inputSplit, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitReader<VectorSchemaRoot> createArrowReader(InputSplit inputSplit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SplitReader<VectorSchemaRoot> createArrowReader(InputSplit inputSplit, int batchSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteCapabilities getWriteCapabilities() {
        return new WriteCapabilities(true, false, true);
    }


    @Override
    public TableWriteSession createWriteSession(String project,
                                                String table,
                                                TableSchema tableSchema,
                                                Map<String, String> partitionSpec,
                                                boolean overwrite,
                                                Options configs) {

        return new TunnelWriteSession(project, table, tableSchema, partitionSpec, overwrite, configs);
    }

    @Override
    public TableWriteSession getWriteSession(WriteSessionInfo writeSessionInfo) {
        return new TunnelWriteSession(writeSessionInfo);
    }

    @Override
    public FileWriter<ArrayRecord> createRecordWriter(WriteSessionInfo sessionInfo,
                                                      Map<String, String> partitionSpec,
                                                      int fileIndex) {
        if (((TunnelWriteSessionInfo) sessionInfo).isStream()) {
            return new TunnelStreamWriter((TunnelWriteSessionInfo) sessionInfo, partitionSpec);
        } else {
            return new TunnelWriter((TunnelWriteSessionInfo) sessionInfo, fileIndex, partitionSpec);
        }
    }

    @Override
    public FileWriter<ColDataBatch> createColDataWriter(WriteSessionInfo sessionInfo,
                                                        Map<String, String> partitionSpec,
                                                        int fileIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileWriter<VectorSchemaRoot> createArrowWriter(WriteSessionInfo sessionInfo,
                                                          Map<String, String> partitionSpec,
                                                          int fileIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileWriter<VectorSchemaRoot> createArrowWriter(WriteSessionInfo sessionInfo,
                                                          Map<String, String> partitionSpec,
                                                          int fileIndex,
                                                          int attemptId) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public TableReadSession createReadSession(String project,
                                              String table,
                                              TableSchema tableSchema,
                                              RequiredSchema readDataColumns,
                                              List<Map<String, String>> readPartitions) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    public TableWriteSession createWriteSession(String project,
                                                String table,
                                                TableSchema tableSchema,
                                                Map<String, String> partitionSpec,
                                                boolean isOverwrite) {
        throw new UnsupportedOperationException();
    }
}
