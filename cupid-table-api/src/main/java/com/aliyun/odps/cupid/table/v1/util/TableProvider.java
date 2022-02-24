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

package com.aliyun.odps.cupid.table.v1.util;

import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.cupid.table.v1.writer.TableWriteSession;
import com.aliyun.odps.cupid.table.v1.writer.WriteCapabilities;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.Map;

public interface TableProvider {

    String getShortName();

    ReadCapabilities getReadCapabilities();

    TableReadSession createReadSession(String project,
                                       String table,
                                       TableSchema tableSchema,
                                       RequiredSchema readDataColumns,
                                       List<Map<String, String>> readPartitions);

    TableReadSession createReadSession(String project,
                                       String table,
                                       TableSchema tableSchema,
                                       RequiredSchema readDataColumns,
                                       List<Map<String, String>> readPartitions,
                                       Options configs);

    SplitReader<ArrayRecord> createRecordReader(InputSplit inputSplit);

    SplitReader<ColDataBatch> createColDataReader(InputSplit inputSplit, int batchSize);

    SplitReader<VectorSchemaRoot> createArrowReader(InputSplit inputSplit);

    SplitReader<VectorSchemaRoot> createArrowReader(InputSplit inputSplit, int batchSize);

    WriteCapabilities getWriteCapabilities();

    TableWriteSession createWriteSession(String project,
                                         String table,
                                         TableSchema tableSchema,
                                         Map<String, String> partitionSpec,
                                         boolean overwrite);

    TableWriteSession createWriteSession(String project,
                                         String table,
                                         TableSchema tableSchema,
                                         Map<String, String> partitionSpec,
                                         boolean overwrite,
                                         Options configs);

    TableWriteSession getWriteSession(WriteSessionInfo writeSessionInfo);

    FileWriter<ArrayRecord> createRecordWriter(WriteSessionInfo sessionInfo,
                                               Map<String, String> partitionSpec,
                                               int fileIndex);

    FileWriter<ColDataBatch> createColDataWriter(WriteSessionInfo sessionInfo,
                                                 Map<String, String> partitionSpec,
                                                 int fileIndex);

    FileWriter<VectorSchemaRoot> createArrowWriter(WriteSessionInfo sessionInfo,
                                                   Map<String, String> partitionSpec,
                                                   int fileIndex);

    FileWriter<VectorSchemaRoot> createArrowWriter(WriteSessionInfo sessionInfo,
                                                   Map<String, String> partitionSpec,
                                                   int fileIndex,
                                                   int attemptId);
}
