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

package com.aliyun.odps.cupid.table.v1.vectorized;

import com.aliyun.odps.cupid.table.v1.util.Validator;

import java.util.Arrays;

public final class ColDataBatch {

    private int rowCount;
    private ColDataVector[] vectors;

    public ColDataBatch(ColDataVector[] vectors) {
        Validator.checkArray(vectors, 0, "vectors");
        this.vectors = vectors;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
        Arrays.stream(this.getVectors()).forEach(colDataVector -> {
            colDataVector.setNumRows(rowCount);
        });
    }

    public int getRowCount() {
        return rowCount;
    }

    public ColDataVector[] getVectors() {
        return vectors;
    }

    public int getColumnCount() {
        return vectors.length;
    }
}
