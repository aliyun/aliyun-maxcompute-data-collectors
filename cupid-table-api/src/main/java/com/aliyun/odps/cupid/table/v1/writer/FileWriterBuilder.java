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

package com.aliyun.odps.cupid.table.v1.writer;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.util.Builder;
import com.aliyun.odps.cupid.table.v1.util.ProviderRegistry;
import com.aliyun.odps.cupid.table.v1.util.Validator;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FileWriterBuilder extends Builder {

    private final WriteSessionInfo sessionInfo;
    private Map<String, String> partitionSpec;
    private int fileIndex = -1;
    private int attemptNumber = -1;

    private boolean built;

    public FileWriterBuilder(WriteSessionInfo sessionInfo, int fileIndex) {
        Validator.checkNotNull(sessionInfo, "sessionInfo");
        Validator.checkInteger(fileIndex, 0,"fileIndex");
        this.sessionInfo = sessionInfo;
        this.fileIndex = fileIndex;
    }

    public FileWriterBuilder attemptNumber(int attemptNumber) {
        Validator.checkInteger(fileIndex, 0,"attemptNumber");
        this.attemptNumber = attemptNumber;
        return this;
    }

    public FileWriterBuilder partitionSpec(Map<String, String> partitionSpec) {
        Validator.checkNotNull(partitionSpec, "partitionSpec");
        this.partitionSpec = partitionSpec;
        return this;
    }

    public FileWriter<ArrayRecord> buildRecordWriter() throws ClassNotFoundException {
        sanitize();
        return ProviderRegistry.lookup(sessionInfo.getProvider()).createRecordWriter(
                sessionInfo, partitionSpec, fileIndex);
    }

    public FileWriter<ColDataBatch> buildColDataWriter() throws ClassNotFoundException {
        sanitize();
        return ProviderRegistry.lookup(sessionInfo.getProvider()).createColDataWriter(
                sessionInfo, partitionSpec, fileIndex);
    }

    public FileWriter<VectorSchemaRoot> buildArrowWriter() throws ClassNotFoundException {
        sanitize();
        if (this.attemptNumber > -1) {
            return ProviderRegistry.lookup(sessionInfo.getProvider()).createArrowWriter(
                    sessionInfo, partitionSpec, fileIndex, attemptNumber);
        } else {
            return ProviderRegistry.lookup(sessionInfo.getProvider()).createArrowWriter(
                    sessionInfo, partitionSpec, fileIndex);
        }
    }

    private void sanitize() {
        markBuilt();

        if (partitionSpec == null) {
            partitionSpec = Collections.emptyMap();
        } else {
            Validator.checkMap(partitionSpec, "partitionSpec");
            Map<String,String>  orderedPartSpec = new LinkedHashMap<>();
            for(Attribute partAttr : sessionInfo.getPartitionColumns()) {
                if (!partitionSpec.containsKey(partAttr.getName())) {
                    throw new RuntimeException("invalid partitionSpec for file writer: " + partitionSpec + ", expected: " + partAttr.getName());
                }
                orderedPartSpec.put(partAttr.getName(), partitionSpec.get(partAttr.getName()));
            }
            partitionSpec = orderedPartSpec;
        }
    }
}
