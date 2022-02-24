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

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.writer.WriteSessionInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoParser;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableUtils {

    public static Column[] toColumnArray(List<Attribute> attributes) {
        Column[] result = null;
        if (attributes != null) {
            ArrayList<Column> columns = new ArrayList<>(attributes.size());
            attributes.forEach(dataCol-> {
                Column column = new Column(dataCol.getName(), getTypeInfoFromString(dataCol.getType()));
                columns.add(column);
            });
            result = columns.toArray(new Column[0]);
        }
        return result;
    }

    public static Map<String,String> getOrderedPartitionSpec(TableSchema tableSchema, Map<String, String> partitionSpec) {
        if (tableSchema == null) {
            throw new RuntimeException("tableSchema cannot be null");
        }
        Map<String,String>  orderedPartSpec = new LinkedHashMap<>();
        tableSchema.getPartitionColumns().stream().forEach(column -> {
            orderedPartSpec.put(column.getName(), partitionSpec.get(column.getName()));
        });
        return orderedPartSpec;
    }

    public static TableSchema toTableSchema(WriteSessionInfo writeSessionInfo) {
        TableSchema tableSchema = new TableSchema();
        if (writeSessionInfo.getDataColumns() != null) {
            ArrayList<Column> dataColumns = new ArrayList<>(writeSessionInfo.getDataColumns().size());
            writeSessionInfo.getDataColumns().forEach(dataCol->{
                Column column = new Column(dataCol.getName(),getTypeInfoFromString(dataCol.getType()));
                dataColumns.add(column);
            });
            tableSchema.setColumns(dataColumns);
        }

        if (writeSessionInfo.getPartitionColumns() != null) {
            ArrayList<Column> partitionColumns = new ArrayList<>(writeSessionInfo.getPartitionColumns().size());
            writeSessionInfo.getPartitionColumns().forEach(partiCol ->{
                Column column = new Column(partiCol.getName(),getTypeInfoFromString(partiCol.getType()));
                partitionColumns.add(column);
            });
            tableSchema.setPartitionColumns(partitionColumns);
        }
        return tableSchema;
    }

    public static TypeInfo getTypeInfoFromString(String typeName) {
        TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString(typeName);
        if (typeInfo == null) {
            throw new RuntimeException("parse odps type info fail");
        }
        return typeInfo;
    }
}
