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

package com.aliyun.odps.cupid.table.v1.reader;

import com.aliyun.odps.cupid.table.v1.util.Builder;
import com.aliyun.odps.cupid.table.v1.util.ProviderRegistry;
import com.aliyun.odps.cupid.table.v1.util.Validator;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.data.ArrayRecord;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class SplitReaderBuilder extends Builder {

    private final InputSplit inputSplit;

    public SplitReaderBuilder(InputSplit inputSplit) {
        Validator.checkNotNull(inputSplit, "inputSplit");
        this.inputSplit = inputSplit;
    }

    public SplitReader<ArrayRecord> buildRecordReader() throws ClassNotFoundException {
        markBuilt();
        return ProviderRegistry.lookup(inputSplit.getProvider()).createRecordReader(inputSplit);
    }

    public SplitReader<ColDataBatch> buildColDataReader(int batchSize) throws ClassNotFoundException {
        markBuilt();
        return ProviderRegistry.lookup(inputSplit.getProvider()).createColDataReader(inputSplit, batchSize);
    }

    public SplitReader<VectorSchemaRoot> buildArrowReader() throws ClassNotFoundException {
        markBuilt();
        return ProviderRegistry.lookup(inputSplit.getProvider()).createArrowReader(inputSplit);
    }

    public SplitReader<VectorSchemaRoot> buildArrowReader(int batchSize) throws ClassNotFoundException {
        markBuilt();
        return ProviderRegistry.lookup(inputSplit.getProvider()).createArrowReader(inputSplit, batchSize);
    }
}
