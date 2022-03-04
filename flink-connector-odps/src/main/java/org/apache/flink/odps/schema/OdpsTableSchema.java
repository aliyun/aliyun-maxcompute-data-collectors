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

package org.apache.flink.odps.schema;

import com.aliyun.odps.Column;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * ODPS table schema information including column information and isPartition table
 */
public class OdpsTableSchema implements Serializable {
    private static final long serialVersionUID = -6327923765714170499L;

    private final List<OdpsColumn> columns;
    private final boolean isPartition;
    private final boolean isView;
    private transient Map<String, OdpsColumn> columnMap;

    public OdpsTableSchema(List<Column> normalColumns, List<Column> partitionColumns, boolean isView) {

        checkArgument(normalColumns != null && !normalColumns.isEmpty(),
                "input normal columns cannot be null or empty!");

        List<OdpsColumn> columnList = normalColumns.stream()
                .map(column -> new OdpsColumn(column.getName(), column.getTypeInfo()))
                .collect(Collectors.toList());
        this.isView = isView;

        boolean hasPartitionCols = partitionColumns != null && !partitionColumns.isEmpty();
        if (hasPartitionCols) {
            List<OdpsColumn> partitionColumnList = partitionColumns.stream()
                    .map(column -> new OdpsColumn(column.getName(), column.getTypeInfo(), true))
                    .collect(Collectors.toList());
            columnList.addAll(partitionColumnList);
        }
        this.isPartition = !isView && hasPartitionCols;
        this.columns = columnList;
        rebuildColumnMap();
    }

    public List<OdpsColumn> getColumns() {
        return columns;
    }

    public boolean isPartition() {
        return isPartition;
    }

    public boolean isView() {
        return isView;
    }

    public OdpsColumn getColumn(String name) {
        return columnMap.get(name);
    }

    public boolean isPartitionColumn(String name) {
        OdpsColumn column = columnMap.get(name);
        if (column != null) {
            return column.isPartition();
        } else {
            throw new IllegalArgumentException("unknown column " + name);
        }
    }

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        rebuildColumnMap();
    }

    private void rebuildColumnMap() {
        this.columnMap = columns.stream().collect(Collectors.toMap(OdpsColumn::getName, column -> column));
    }
}

