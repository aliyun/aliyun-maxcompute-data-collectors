/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.util;

import com.aliyun.odps.data.*;
import com.aliyun.odps.type.*;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class RowDataToOdpsConverters {

    @FunctionalInterface
    public interface RowDataToOdpsRecordConverter extends Serializable {
        void convert(Object in, ArrayRecord out);
    }

    @FunctionalInterface
    public interface RowDataToOdpsFieldConverter extends Serializable {
        Object convert(TypeInfo odpsTypeInfo, Object object);
    }

    public static RowDataToOdpsRecordConverter createRecordConverter(RowType rowType) {
        final RowDataToOdpsFieldConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(RowDataToOdpsConverters::createConverter)
                        .toArray(RowDataToOdpsFieldConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        return new RowDataToOdpsRecordConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public void convert(Object in, ArrayRecord out) {
                final RowData row = (RowData) in;
                for (int i = 0; i < out.getColumnCount(); ++i) {
                    Object valueObject =
                            fieldConverters[i].convert(
                                    out.getColumns()[i].getTypeInfo(),
                                    fieldGetters[i].getFieldOrNull(row));
                    out.set(i, valueObject);
                }
            }
        };
    }

    public static RowDataToOdpsFieldConverter createConverter(LogicalType type) {
        final RowDataToOdpsFieldConverter converter;
        switch (type.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        return object;
                    }
                };
                break;
            case VARBINARY:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;
                    // TODO: var binary has data copy ?
                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        return new Binary((byte[])object);
                    }
                };
                break;
            case CHAR:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        return new Char(object.toString(),
                                ((CharTypeInfo) typeInfo).getLength());
                    }
                };
                break;
            case VARCHAR:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        switch (typeInfo.getOdpsType()) {
                            case VARCHAR:
                                return new Varchar(object.toString(),
                                        ((VarcharTypeInfo) typeInfo).getLength());
                            case STRING:
                                return object.toString();
                            default:
                                throw new UnsupportedOperationException("Unsupported odps type: "
                                        + typeInfo.getTypeName());
                        }
                    }
                };
                break;
            case DECIMAL:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        return ((DecimalData) object).toBigDecimal();
                    }
                };
                break;
            case DATE:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        return LocalDate.ofEpochDay((int) object);
                    }
                };
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                converter = new RowDataToOdpsFieldConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(TypeInfo typeInfo, Object object) {
                        TimestampData timestamp = (TimestampData) object;
                        switch (typeInfo.getOdpsType()) {
                            case DATETIME:
                                return timestamp.toInstant().atZone(ZoneId.systemDefault());
                            case TIMESTAMP:
                                return timestamp.toInstant();
                            default:
                                throw new UnsupportedOperationException("Unsupported odps type: "
                                        + typeInfo.getTypeName());
                        }
                    }
                };
                break;
            case ARRAY:
                converter = createArrayConverter((ArrayType) type);
                break;
            case ROW:
                converter = createRowConverter((RowType) type);
                break;
            case MAP:
                converter = createMapConverter((MapType) type);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
        // wrap into nullable converter
        return new RowDataToOdpsFieldConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(TypeInfo typeInfo, Object object) {
                if (object == null) {
                    return null;
                }
                return converter.convert(typeInfo, object);
            }
        };
    }

    private static RowDataToOdpsFieldConverter createRowConverter(RowType rowType) {
        final RowDataToOdpsFieldConverter[] fieldConverters =
                rowType.getChildren().stream()
                        .map(RowDataToOdpsConverters::createConverter)
                        .toArray(RowDataToOdpsFieldConverter[]::new);
        final LogicalType[] fieldTypes =
                rowType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }
        final int length = rowType.getFieldCount();
        return new RowDataToOdpsFieldConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(TypeInfo typeInfo, Object object) {
                final RowData row = (RowData) object;
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                List<Object> result = new ArrayList<>(row.getArity());
                for (int i = 0; i < length; ++i) {
                    final TypeInfo fieldType = structTypeInfo.getFieldTypeInfos().get(i);
                    Object odpsObject =
                            fieldConverters[i].convert(
                                    fieldType, fieldGetters[i].getFieldOrNull(row));
                    result.add(odpsObject);
                }
                return new SimpleStruct(structTypeInfo, result);
            }
        };
    }

    private static RowDataToOdpsFieldConverter createArrayConverter(ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        final RowDataToOdpsFieldConverter elementConverter = createConverter(arrayType.getElementType());
        return new RowDataToOdpsFieldConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(TypeInfo typeInfo, Object object) {
                final TypeInfo elementType = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();
                ArrayData arrayData = (ArrayData) object;
                List<Object> list = new ArrayList<>();
                for (int i = 0; i < arrayData.size(); ++i) {
                    list.add(elementConverter.convert(elementType, elementGetter.getElementOrNull(arrayData, i)));
                }
                return list;
            }
        };
    }

    private static RowDataToOdpsFieldConverter createMapConverter(MapType type) {
        LogicalType valueType = type.getValueType();
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
        final RowDataToOdpsFieldConverter valueConverter = createConverter(valueType);

        return new RowDataToOdpsFieldConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(TypeInfo typeInfo, Object object) {
                final TypeInfo valueType = ((MapTypeInfo) typeInfo).getValueTypeInfo();
                final MapData mapData = (MapData) object;
                final ArrayData keyArray = mapData.keyArray();
                final ArrayData valueArray = mapData.valueArray();
                final Map<Object, Object> map = new HashMap<>(mapData.size());
                for (int i = 0; i < mapData.size(); ++i) {
                    final String key = keyArray.getString(i).toString();
                    final Object value =
                            valueConverter.convert(
                                    valueType, valueGetter.getElementOrNull(valueArray, i));
                    map.put(key, value);
                }
                return map;
            }
        };
    }
}

