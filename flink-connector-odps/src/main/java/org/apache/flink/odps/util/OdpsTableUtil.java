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

package org.apache.flink.odps.util;

import com.aliyun.odps.Column;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

/**
 * Utils for Odps table.
 */
public class OdpsTableUtil {

    private OdpsTableUtil() {
    }

    /**
     * Create a Flink's TableSchema from Odps table's columns and partition keys.
     */
    public static TableSchema createTableSchema(List<Column> cols, List<Column> partitionKeys) {
        List<Column> allCols = new ArrayList<>(cols);
        allCols.addAll(partitionKeys);

        String[] colNames = new String[allCols.size()];
        DataType[] colTypes = new DataType[allCols.size()];

        for (int i = 0; i < allCols.size(); i++) {
            Column col = allCols.get(i);

            colNames[i] = col.getName();
            colTypes[i] = OdpsTypeUtil.toFlinkType(col.getTypeInfo());
            if (col.isNullable()) {
                colTypes[i] = colTypes[i].nullable();
            } else {
                colTypes[i] = colTypes[i].notNull();
            }
        }
        return TableSchema.builder()
                .fields(colNames, colTypes)
                .build();
    }

    /**
     * Create Odps columns from Flink TableSchema.
     */
    public static List<Column> createOdpsColumns(TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldTypes = schema.getFieldDataTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);

        for (int i = 0; i < fieldNames.length; i++) {
            Column column = new Column(fieldNames[i], OdpsTypeUtil.toOdpsTypeInfo(fieldTypes[i]));
            column.setNullable(fieldTypes[i].getLogicalType().isNullable());
            columns.add(column);
        }

        return columns;
    }

    public static DataType toRowDataType(List<Column> columns) {
        final DataTypes.Field[] fields =
                columns.stream()
                        .map(column -> FIELD(column.getName(),
                                OdpsTypeUtil.toFlinkType(column.getTypeInfo())))
                        .toArray(DataTypes.Field[]::new);
        return ROW(fields).notNull();
    }
}
