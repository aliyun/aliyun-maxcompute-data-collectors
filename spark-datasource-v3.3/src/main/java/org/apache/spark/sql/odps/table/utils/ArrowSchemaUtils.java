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

package org.apache.spark.sql.odps.table.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.table.configuration.ArrowOptions;
import com.aliyun.odps.type.*;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.aliyun.odps.table.utils.SchemaUtils.ARROW_DECIMAL_DEFAULT_PRECISION;
import static com.aliyun.odps.table.utils.SchemaUtils.ARROW_DECIMAL_DEFAULT_SCALE;

public class ArrowSchemaUtils {

    public static Schema toArrowSchema(List<Column> columns) {
        return toArrowSchema(columns, ArrowOptions.createDefault());
    }

    public static Field columnToArrowField(Column fieldColumn) {
        return columnToArrowField(fieldColumn, ArrowOptions.createDefault());
    }

    public static Schema toArrowSchema(List<Column> columns,
                                       ArrowOptions arrowOptions) {
        Collection<Field> fields =
                columns.stream().map(col -> columnToArrowField(col, arrowOptions))
                        .collect(Collectors.toCollection(ArrayList::new));
        return new Schema(fields);
    }

    public static Field columnToArrowField(Column fieldColumn,
                                           ArrowOptions options) {
        String fieldName = fieldColumn.getName();
        TypeInfo typeInfo = fieldColumn.getTypeInfo();
        return convertTypeInfoToArrowField(fieldName, typeInfo, fieldColumn.isNullable(), options);
    }

    private static Field convertTypeInfoToArrowField(String fieldName,
                                                     TypeInfo typeInfo,
                                                     boolean nullable,
                                                     ArrowOptions options) {
        ArrowType arrowType = getArrowType(typeInfo, options);
        return new Field(fieldName, new FieldType(nullable, arrowType, null, null),
                generateSubFields(typeInfo, options));
    }

    private static List<Field> generateSubFields(TypeInfo typeInfo,
                                                 ArrowOptions options) {
        if (typeInfo instanceof ArrayTypeInfo) {
            ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
            TypeInfo subti = arrayTypeInfo.getElementTypeInfo();
            return Arrays.asList(convertTypeInfoToArrowField("element", subti, true, options));
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
            TypeInfo keyti = mapTypeInfo.getKeyTypeInfo(), valti = mapTypeInfo.getValueTypeInfo();
            return Arrays.asList(
                    new Field("element", new FieldType(false, new ArrowType.Struct(), null, null),
                            Arrays.asList(
                                    convertTypeInfoToArrowField("key", keyti, false, options),
                                    convertTypeInfoToArrowField("value", valti, true, options)
                            )
                    )
            );
        } else if (typeInfo instanceof StructTypeInfo) {
            StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
            ArrayList<Field> sfields = new ArrayList<>();
            List<TypeInfo> subTypeInfos = structTypeInfo.getFieldTypeInfos();
            List<String> subNames = structTypeInfo.getFieldNames();
            for (int i = 0; i < structTypeInfo.getFieldCount(); i++) {
                sfields.add(convertTypeInfoToArrowField(subNames.get(i), subTypeInfos.get(i), true, options));
            }
            return sfields;
        } else {
            return null;
        }
    }

    private static ArrowType getArrowType(TypeInfo typeInfo,
                                          ArrowOptions options) {
        ArrowType arrowType = null;
        switch (typeInfo.getOdpsType()) {
            case CHAR:
            case VARCHAR:
            case STRING:
                arrowType = new ArrowType.Utf8();
                break;
            case BINARY:
                arrowType = new ArrowType.Binary();
                break;
            case TINYINT:
                arrowType = new ArrowType.Int(8, true);
                break;
            case SMALLINT:
                arrowType = new ArrowType.Int(16, true);
                break;
            case INT:
                arrowType = new ArrowType.Int(32, true);
                break;
            case BIGINT:
                arrowType = new ArrowType.Int(64, true);
                break;
            case BOOLEAN:
                arrowType = new ArrowType.Bool();
                break;
            case FLOAT:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
                break;
            case DOUBLE:
                arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
                break;
            case DECIMAL:
                // TODO: decimal
                if (((DecimalTypeInfo) typeInfo).getPrecision() == 54 &&
                        ((DecimalTypeInfo) typeInfo).getScale() == 18) {
                    arrowType = new ArrowType.Decimal(ARROW_DECIMAL_DEFAULT_PRECISION, ARROW_DECIMAL_DEFAULT_SCALE);
                } else {
                    arrowType = new ArrowType.Decimal(((DecimalTypeInfo) typeInfo).getPrecision(), ((DecimalTypeInfo) typeInfo).getScale());
                }
                break;
            case DATE:
                arrowType = new ArrowType.Date(DateUnit.DAY);
                break;
            case DATETIME:
                // for tunnel arrow date time
                arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
                break;
            case TIMESTAMP:
                arrowType = parseTimeStamp(options.getTimestampUnit());
                break;
            case ARRAY:
                arrowType = new ArrowType.List();
                break;
            case STRUCT:
                arrowType = new ArrowType.Struct();
                break;
            case MAP:
                arrowType = new ArrowType.Map(false);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + typeInfo.getOdpsType());
        }
        return arrowType;
    }

    private static ArrowType.Timestamp parseTimeStamp(ArrowOptions.TimestampUnit unit) {
        switch (unit) {
            case SECOND:
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            case MILLI:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case NANO:
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            case MICRO:
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + unit);
        }
    }
}
