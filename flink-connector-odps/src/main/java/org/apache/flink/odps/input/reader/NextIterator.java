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

package org.apache.flink.odps.input.reader;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.util.OdpsTypeConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public interface NextIterator<T> extends Iterator<T>, Closeable {
    /**
     * Seek to a particular row number.
     */
    default T seekToRow(long rowCount) throws IOException {
        T reuse = null;
        for (int i = 0; i < rowCount; i++) {
            boolean end = hasNext();
            if (!end) {
                throw new FlinkOdpsException("Seek too many rows.");
            }
            reuse = next();
        }
        return reuse;
    }

    default RowData buildFlinkRowData(Record record, Column[] fullColumns, OdpsTypeConverter[] typeConverters) {
        GenericRowData genericRowData = new GenericRowData(fullColumns.length);
        for (int i = 0; i < genericRowData.getArity(); i++) {
            Object flinkField = typeConverters[i].toFlinkDataField(record, fullColumns[i]);
            genericRowData.setField(i, flinkField);
        }
        return genericRowData;
    }

    default Row buildFlinkRow(T reuse, Record record, Column[] fullColumns, OdpsTypeConverter[] typeConverters) {
        Row row = (Row) reuse;
        if (row.getArity() > fullColumns.length) {
            throw new FlinkOdpsException("Row arity cannot greater than record length");
        }
        for (int i = 0; i < row.getArity(); i++) {
            Object flinkField = typeConverters[i].toFlinkField(record, fullColumns[i]);
            row.setField(i, flinkField);
        }
        return row;
    }

    default Tuple buildFlinkTuple(T reuse, Record record, Column[] fullColumns, OdpsTypeConverter[] typeConverters) {
        Tuple tuple = (Tuple) reuse;
        if (tuple.getArity() > fullColumns.length) {
            throw new FlinkOdpsException("Tuple arity cannot greater than record length");
        }
        for (int i = 0; i < tuple.getArity(); i++) {
            Object flinkField = typeConverters[i].toFlinkField(record, fullColumns[i]);
            tuple.setField(flinkField, i);
        }
        return tuple;
    }

    default Object buildFlinkPojo(T reuse, Record record, Column[] fullColumns) {
        Field[] fields = reuse.getClass().getFields();
        Map<String, Column> columnMap = Arrays
                .stream(fullColumns)
                .collect(Collectors.toMap(Column::getName, column -> column));
        for (Field field : fields) {
            String fieldName = field.getName();
            String columnName = fieldName.toLowerCase();
            if (!columnMap.containsKey(columnName)) {
                continue;
            }
            OdpsTypeConverter odpsTypeConverter = OdpsTypeConverter.valueOf(columnMap.get(columnName).getType().name());
            Object flinkField = odpsTypeConverter.toFlinkField(record, columnMap.get(columnName));
            String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            Method method;
            try {
                method = reuse.getClass().getMethod(methodName, new Class[]{field.getType()});
                method.invoke(reuse, new Object[]{flinkField});
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                try {
                    reuse.getClass().getField(fieldName).set(reuse, flinkField);
                } catch (Exception e2) {
                    throw new FlinkOdpsException("odps record cannot match pojo", e2);
                }
            }
        }
        return reuse;
    }

    void setReuse(T reuse);
}