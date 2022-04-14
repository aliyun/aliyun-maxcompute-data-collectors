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
import com.aliyun.odps.data.*;
import com.aliyun.odps.type.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.*;
import org.apache.flink.types.Row;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;


public enum OdpsTypeConverter {

    /**
     * Type of a 1-byte signed integer with values from -128 to 127.
     */
    TINYINT {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.BYTE_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Byte.valueOf(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Short) {
                throw new ClassCastException("Short value: " + sourceValue + " can not be cast to Byte");
            } else if (sourceValue instanceof Integer) {
                throw new ClassCastException("Integer value: " + sourceValue + " can not be cast to Byte");
            } else if (sourceValue instanceof Long) {
                throw new ClassCastException("Long value: " + sourceValue + " can not be cast to Byte");
            } else if (sourceValue instanceof Float) {
                throw new ClassCastException("Float value: " + sourceValue + " can not be cast to Byte");
            } else if (sourceValue instanceof Double) {
                throw new ClassCastException("Double value: " + sourceValue + " can not be cast to Byte");
            } else if (sourceValue instanceof Boolean) {
                return (byte) (((Boolean) sourceValue) ? 1 : 0);
            } else if (sourceValue instanceof Character) {
                return ((byte) ((Character) sourceValue).charValue());
            } else if (sourceValue instanceof String) {
                return Byte.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a 2-byte signed integer with values from -32,768 to 32,767.
     */
    SMALLINT() {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.SHORT_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Short.valueOf(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Integer) {
                throw new ClassCastException("Integer value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Long) {
                throw new ClassCastException("Long value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Float) {
                throw new ClassCastException("Float value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Double) {
                throw new ClassCastException("Double value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Boolean) {
                return (short) (((Boolean) sourceValue) ? 1 : 0);
            } else if (sourceValue instanceof Character) {
                return ((short) ((Character) sourceValue).charValue());
            } else if (sourceValue instanceof String) {
                return Short.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a 4-byte signed integer with values from -2,147,483,648 to 2,147,483,647.
     */
    INT {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.INT_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Integer.valueOf(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Long) {
                throw new ClassCastException("Long value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Float) {
                throw new ClassCastException("Float value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Double) {
                throw new ClassCastException("Double value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Boolean) {
                return ((Boolean) sourceValue) ? 1 : 0;
            } else if (sourceValue instanceof Character) {
                return ((int) (Character) sourceValue);
            } else if (sourceValue instanceof String) {
                return Integer.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of an 8-byte signed integer with values from -9,223,372,036,854,775,808 to
     * * 9,223,372,036,854,775,807.
     */
    BIGINT {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.LONG_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Long.parseLong(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBigint(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBigint(sourceColumn.getName());
        }


        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Float) {
                throw new ClassCastException("Float value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Double) {
                throw new ClassCastException("Double value: " + sourceValue + " can not be cast to Short");
            } else if (sourceValue instanceof Boolean) {
                return ((Boolean) sourceValue) ? 1 : 0;
            } else if (sourceValue instanceof Character) {
                return ((long) (Character) sourceValue);
            } else if (sourceValue instanceof String) {
                return Long.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a 4-byte single precision floating point number.
     */
    FLOAT {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.FLOAT_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Float.parseFloat(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.get(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Double) {
                throw new ClassCastException("Double value: " + sourceValue + " can not be cast to Float");
            } else if (sourceValue instanceof Boolean) {
                return ((Boolean) sourceValue) ? 1.0f : 0f;
            } else if (sourceValue instanceof Character) {
                return sourceValue;
            } else if (sourceValue instanceof String) {
                return Float.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of an 8-byte double precision floating point number.
     */
    DOUBLE {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.DOUBLE_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Double.parseDouble(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getDouble(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getDouble(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Boolean) {
                return ((Boolean) sourceValue) ? 1.0d : 0d;
            } else if (sourceValue instanceof Character) {
                return sourceValue;
            } else if (sourceValue instanceof String) {
                return Double.valueOf((String) sourceValue);
            }
            return sourceValue;
        }

    },

    /**
     * Type of a boolean with a (possibly) two-valued logic of {@code TRUE, FALSE}.
     */
    BOOLEAN {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr == null ? null : Boolean.parseBoolean(sourceValueStr);
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBoolean(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBoolean(sourceColumn.getName());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Byte) {
                return (Byte) sourceValue != 0;
            } else if (sourceValue instanceof Short) {
                return (Short) sourceValue != 0;
            } else if (sourceValue instanceof Integer) {
                return (Integer) sourceValue != 0;
            } else if (sourceValue instanceof Long) {
                return (Long) sourceValue != 0;
            } else if (sourceValue instanceof Float) {
                return (Float) sourceValue != 0;
            } else if (sourceValue instanceof Double) {
                return (Double) sourceValue != 0;
            } else if (sourceValue instanceof Character) {
                return (Character) sourceValue != 0;
            } else if (sourceValue instanceof String) {
                return Boolean.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a decimal number with fixed precision and scale.
     * NOTE: ODPS decimal type could support max precision 54, while Blink could only support 38.
     */
    DECIMAL {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.BIG_DEC_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be Decimal type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getDecimal(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.getDecimal(sourceColumn.getName()),
                    sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            } else {
                DecimalTypeInfo typeInfo = (DecimalTypeInfo) odpsTypeInfo;
                return DecimalData.fromBigDecimal((BigDecimal) sourceValue,
                        typeInfo.getPrecision(),
                        typeInfo.getScale());
            }
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Byte) {
                return new BigDecimal((int) sourceValue);
            } else if (sourceValue instanceof Short) {
                return new BigDecimal((int) sourceValue);
            } else if (sourceValue instanceof Integer) {
                return new BigDecimal((int) sourceValue);
            } else if (sourceValue instanceof Long) {
                return new BigDecimal((long) sourceValue);
            } else if (sourceValue instanceof Float) {
                return new BigDecimal((double) sourceValue);
            } else if (sourceValue instanceof Double) {
                return new BigDecimal((double) sourceValue);
            } else if (sourceValue instanceof Character) {
                return new BigDecimal((int) sourceValue);
            } else if (sourceValue instanceof String) {
                return new BigDecimal((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a fixed-length binary string (=a sequence of bytes).
     */
    BINARY {
        public TypeInformation<?> toFlinkType() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be Binary type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBytes(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getBytes(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            if (sourceValue instanceof Binary) {
                return ((Binary) sourceValue).data();
            } else {
                return sourceValue;
            }
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            try {
                if (sourceValue == null) {
                    return null;
                } else if (sourceValue instanceof byte[]) {
                    return (byte[]) (sourceValue);
                } else if (sourceValue instanceof String) {
                    return ((String) sourceValue).getBytes("utf-8");
                } else if (sourceValue instanceof Binary) {
                    return ((Binary) sourceValue).data();
                } else if (sourceValue instanceof AbstractChar) {
                    return (((AbstractChar) sourceValue).getValue()).getBytes("utf-8");
                } else {
                    throw new RuntimeException("Does not support getBytes for type other than String/Binary/Char/VarChar, sees " + sourceValue.getClass());
                }
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof byte[]) {
                return new Binary((byte[]) sourceValue);
            } else if (sourceValue instanceof String) {
                return new Binary(((String) sourceValue).getBytes());
            }
            return sourceValue;
        }
    },

    CHAR {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be Binary type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            return StringData.fromString((String) toFlinkField(sourceValue, odpsTypeInfo));
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            return ((Char) sourceValue).getValue();
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof String) {
                return new Char((String) sourceValue, ((CharTypeInfo) odpsTypeInfo).getLength());
            }
            return sourceValue;
        }
    },

    /**
     * Type of a variable-length character string. length from 1 to 65535.
     */
    VARCHAR {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr;
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            return StringData.fromString((String) toFlinkField(sourceValue, odpsTypeInfo));
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            return ((Varchar) sourceValue).getValue();
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof String) {
                return new Varchar((String) sourceValue, ((VarcharTypeInfo) odpsTypeInfo).getLength());
            }
            return sourceValue;
        }
    },

    /**
     * Type of string，Upper limitation is 8M.
     */
    STRING {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            return sourceValueStr;
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return sourceRecord.getString(sourceColumn.getName());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.getString(sourceColumn.getName()),
                    sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            return StringData.fromString((String) sourceValue);
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof Byte) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Short) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Integer) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Long) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Float) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Double) {
                return String.valueOf(sourceValue);
            } else if (sourceValue instanceof Character) {
                return String.valueOf(sourceValue);
            }
            return sourceValue;
        }

    },

    /**
     * Type of a timestamp WITH GMT time zone consisting of {@code year-month-day hour:minute:second[.fractional]}
     * with up to millisecond precision and values ranging from {@code 0000-01-01 00:00:00.000} to
     * {@code 9999-12-31 23:59:59.999}.
     * TODO take TimeZone into consideration.
     */
    DATETIME {
        public TypeInformation<?> toFlinkType() {
            return BasicTypeInfo.DATE_TYPE_INFO;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be DATETIME type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Instant instant = ((Date) sourceValue).toInstant();
            return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant,
                    ZoneId.systemDefault()));
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Instant instant = ((Date) sourceValue).toInstant();
            ZoneId zone = ZoneId.systemDefault();
            return LocalDateTime.ofInstant(instant, zone);
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof LocalDateTime) {
                return Date.from(((LocalDateTime) sourceValue).atZone(ZoneId.systemDefault()).toInstant());
            }
            return sourceValue;
        }
    },

    DATE {
        public TypeInformation<?> toFlinkType() {
            return SqlTimeTypeInfo.DATE;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be DATE type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            if (sourceValue instanceof LocalDate) {
                return (int) ((LocalDate) sourceValue).toEpochDay();
            } else {
                return (int) ((java.sql.Date) sourceValue).toLocalDate().toEpochDay();
            }
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            if (sourceValue instanceof LocalDate) {
                return sourceValue;
            } else {
                return ((java.sql.Date) sourceValue).toLocalDate();
            }
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof LocalDate) {
                return new java.sql.Date(((LocalDate)sourceValue).toEpochDay() * (3600 * 24 * 1000L));
            } else if (sourceValue instanceof String) {
                return java.sql.Date.valueOf((String) sourceValue);
            }
            return sourceValue;
        }
    },

    /**
     * Type of a timestamp WITHOUT time zone consisting of {@code year-month-day hour:minute:second[.fractional]}
     * with up to nanosecond precision and values ranging from {@code 0000-01-01 00:00:00.000000000} to
     * {@code 9999-12-31 23:59:59.999999999}.
     * <p>
     * Note: Timestamp in Blink only support millisecond precision.
     */
    TIMESTAMP {
        public TypeInformation<?> toFlinkType() {
            return SqlTimeTypeInfo.TIMESTAMP;
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be TIMESTAMP type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(((ArrayRecord) sourceRecord).getTimestamp(sourceColumn.getName()),
                    sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(((ArrayRecord) sourceRecord).getTimestamp(sourceColumn.getName()),
                    sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Timestamp timestamp = (Timestamp) sourceValue;
            return TimestampData.fromTimestamp(timestamp);
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Timestamp timestamp = (Timestamp) sourceValue;
            return LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue instanceof LocalDateTime) {
                return Timestamp.valueOf((LocalDateTime) sourceValue);
            }
            return sourceValue;
        }
    },

    STRUCT {
        public TypeInformation<?> toFlinkType() {
            throw new UnsupportedOperationException("STRUCT type is not supported now!");
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be STRUCT type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Struct field = (Struct) sourceValue;
            GenericRowData row = new GenericRowData(field.getFieldCount());
            for (int i = 0; i < field.getFieldCount(); ++i) {
                TypeInfo fieldTypeInfo = field.getFieldTypeInfo(i);
                OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(fieldTypeInfo.getOdpsType().name());
                row.setField(i, eleConverter.toFlinkDataField(field.getFieldValue(i), fieldTypeInfo));
            }
            return row;
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Struct field = (Struct) sourceValue;
            Row row = new Row(field.getFieldCount());
            for (int i = 0; i < field.getFieldCount(); ++i) {
                TypeInfo fieldTypeInfo = field.getFieldTypeInfo(i);
                OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(fieldTypeInfo.getOdpsType().name());
                row.setField(i, eleConverter.toFlinkField(field.getFieldValue(i), fieldTypeInfo));
            }
            return row;
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Row row = (Row) sourceValue;
            StructTypeInfo structTypeInfo = ((StructTypeInfo) odpsTypeInfo);
            List<Object> result = new ArrayList<>(row.getArity());
            for (int i = 0; i < structTypeInfo.getFieldCount(); ++i) {
                TypeInfo fieldTypeInfo = structTypeInfo.getFieldTypeInfos().get(i);
                OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(fieldTypeInfo.getOdpsType().name());
                result.add(eleConverter.toOdpsField(row.getField(i), fieldTypeInfo));
            }
            return new SimpleStruct(structTypeInfo, result);
        }

    },

    MAP {
        public TypeInformation<?> toFlinkType() {
            throw new UnsupportedOperationException("Map type is not supported now!");
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be Map type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Map<Object, Object> sourceMap = (Map) sourceValue;
            MapTypeInfo mapTypeInfo = (MapTypeInfo) odpsTypeInfo;
            Map<Object, Object> result = new HashMap<>();
            OdpsTypeConverter keyConverter = OdpsTypeConverter.valueOf(mapTypeInfo.getKeyTypeInfo().getOdpsType().name());
            OdpsTypeConverter valConverter = OdpsTypeConverter.valueOf(mapTypeInfo.getValueTypeInfo().getOdpsType().name());
            for (Map.Entry<Object, Object> entry : sourceMap.entrySet()) {
                result.put(
                        keyConverter.toFlinkDataField(entry.getKey(), mapTypeInfo.getKeyTypeInfo()),
                        valConverter.toFlinkDataField(entry.getValue(), mapTypeInfo.getValueTypeInfo()));
            }
            return new GenericMapData(result);
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            Map<Object, Object> sourceMap = (Map) sourceValue;
            MapTypeInfo mapTypeInfo = (MapTypeInfo) odpsTypeInfo;
            Map<Object, Object> result = new HashMap<>();
            OdpsTypeConverter keyConverter = OdpsTypeConverter.valueOf(mapTypeInfo.getKeyTypeInfo().getOdpsType().name());
            OdpsTypeConverter valConverter = OdpsTypeConverter.valueOf(mapTypeInfo.getValueTypeInfo().getOdpsType().name());
            for (Map.Entry<Object, Object> entry : sourceMap.entrySet()) {
                result.put(
                        keyConverter.toFlinkField(entry.getKey(), mapTypeInfo.getKeyTypeInfo()),
                        valConverter.toFlinkField(entry.getValue(), mapTypeInfo.getValueTypeInfo()));
            }
            return result;
        }

        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            TypeInfo keyTypeInfo = ((MapTypeInfo) odpsTypeInfo).getKeyTypeInfo();
            TypeInfo valTypeInfo = ((MapTypeInfo) odpsTypeInfo).getValueTypeInfo();

            OdpsTypeConverter keyConverter = OdpsTypeConverter.valueOf(keyTypeInfo.getOdpsType().name());
            OdpsTypeConverter valConverter = OdpsTypeConverter.valueOf(valTypeInfo.getOdpsType().name());


            Map<Object, Object> map = (Map) sourceValue;
            Map<Object, Object> result = new HashMap<>(map.size());

            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                result.put(
                        keyConverter.toOdpsField(entry.getKey(), keyTypeInfo),
                        valConverter.toOdpsField(entry.getValue(), valTypeInfo));
            }
            return result;
        }
    },

    ARRAY {
        public TypeInformation<?> toFlinkType() {
            throw new UnsupportedOperationException("Array type is not supported now!");
        }

        public Object getPartitionValue(String sourceValueStr) {
            throw new UnsupportedOperationException("Partition column cannot be Array type!");
        }

        @Override
        public Object toFlinkField(Record sourceRecord, Column sourceColumn) {
            return toFlinkField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkDataField(Record sourceRecord, Column sourceColumn) {
            return toFlinkDataField(sourceRecord.get(sourceColumn.getName()), sourceColumn.getTypeInfo());
        }

        @Override
        public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            List<Object> sourceList = (List) sourceValue;
            TypeInfo elementTypeInfo = ((ArrayTypeInfo) odpsTypeInfo).getElementTypeInfo();
            OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(elementTypeInfo.getOdpsType().name());

            Object arr = Array.newInstance(eleConverter.toFlinkField(sourceList.get(0), elementTypeInfo).getClass(), sourceList.size());
            for (int i = 0; i < sourceList.size(); i++) {
                Array.set(arr, i, eleConverter.toFlinkField(sourceList.get(i), elementTypeInfo));
            }
            return arr;
        }

        @Override
        public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            List<Object> sourceList = (List) sourceValue;
            TypeInfo elementTypeInfo = ((ArrayTypeInfo) odpsTypeInfo).getElementTypeInfo();
            OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(elementTypeInfo.getOdpsType().name());
            Object arr = Array.newInstance(eleConverter.toFlinkDataField(sourceList.get(0), elementTypeInfo).getClass(), sourceList.size());
            for (int i = 0; i < sourceList.size(); i++) {
                Array.set(arr, i, eleConverter.toFlinkDataField(sourceList.get(i), elementTypeInfo));
            }
            return new GenericArrayData((Object[]) arr);
        }


        @Override
        public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
            if (sourceValue == null) {
                return null;
            }
            TypeInfo elementTypeInfo = ((ArrayTypeInfo) odpsTypeInfo).getElementTypeInfo();
            OdpsTypeConverter eleConverter = OdpsTypeConverter.valueOf(elementTypeInfo.getOdpsType().name());
            Object[] array = (Object[]) sourceValue;
            List<Object> result = new ArrayList<>();

            for (Object ele : array) {
                result.add(eleConverter.toOdpsField(ele, elementTypeInfo));
            }
            return result;
        }
    };

    /**
     * Mapping from odps type to flink type information
     *
     * @return related flink type information
     */
    public abstract TypeInformation<?> toFlinkType();

    public abstract Object getPartitionValue(String sourceValueStr);

    public abstract Object toFlinkField(Record sourceRecord, Column sourceColumn);

    public abstract Object toFlinkDataField(Record sourceRecord, Column sourceColumn);

    public Object toFlinkDataField(Object sourceValue, TypeInfo odpsTypeInfo) {
        return sourceValue;
    }

    public Object toFlinkField(Object sourceValue, TypeInfo odpsTypeInfo) {
        return sourceValue;
    }

    public Object toOdpsField(Object sourceValue, TypeInfo odpsTypeInfo) {
        return sourceValue;
    }
}
