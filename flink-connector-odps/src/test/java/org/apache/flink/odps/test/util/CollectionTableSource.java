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

package org.apache.flink.odps.test.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.util.Collection;

public class CollectionTableSource extends InputFormatTableSource<Row> {

    private final Collection<Row> data;
    private final RowTypeInfo rowTypeInfo;
    private TableSchema tableSchema;

    public CollectionTableSource(Collection<Row> data, RowTypeInfo rowTypeInfo, TableSchema tableSchema) {
        this.data = data;
        this.rowTypeInfo = rowTypeInfo;
        this.tableSchema = tableSchema;
    }

    @Override
    public DataType getProducedDataType() {
        return tableSchema.toRowDataType();
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return rowTypeInfo;
    }

    @Override
    public InputFormat<Row, ?> getInputFormat() {
        return new CollectionInputFormat<>(data, rowTypeInfo.createSerializer(new ExecutionConfig()));
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }
}