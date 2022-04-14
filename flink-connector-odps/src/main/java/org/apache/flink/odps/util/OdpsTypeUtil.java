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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.odps.util.Constants.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utils to convert data types between Flink and Odps.
 */
public class OdpsTypeUtil {

    private OdpsTypeUtil() {
    }

    /**
     * Convert Flink data type to Odps data type name.
     *
     * @param type a Flink data type
     * @return the corresponding Odps data type name
     */
    public static String toOdpsTypeName(DataType type) {
        checkNotNull(type, "type cannot be null");

        return toOdpsTypeInfo(type).getTypeName();
    }

    /**
     * Convert Flink data type to Odps data type.
     *
     * @param dataType a Flink data type
     * @return the corresponding Odps data type
     */
    public static TypeInfo toOdpsTypeInfo(DataType dataType) {
        checkNotNull(dataType, "type cannot be null");
        LogicalType logicalType = dataType.getLogicalType();
        return logicalType.accept(new TypeInfoLogicalTypeVisitor(dataType));
    }

    /**
     * Convert Odps data type to a Flink data type.
     *
     * @param odpsType a Odps data type
     * @return the corresponding Flink data type
     */
    public static DataType toFlinkType(TypeInfo odpsType) {
        checkNotNull(odpsType, "odpsType cannot be null");
        switch (odpsType.getOdpsType()) {
            case CHAR:
                return DataTypes.CHAR(((CharTypeInfo) odpsType).getLength());
            case VARCHAR:
                return DataTypes.VARCHAR(((VarcharTypeInfo) odpsType).getLength());
            case STRING:
                return DataTypes.STRING();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                return DataTypes.TINYINT();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP(3);
            case TIMESTAMP:
                return DataTypes.TIMESTAMP(9);
            case BINARY:
                return DataTypes.BYTES();
            case DECIMAL:
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) odpsType;
                return DataTypes.DECIMAL(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale());
            case ARRAY:
                ArrayTypeInfo listTypeInfo = (ArrayTypeInfo) odpsType;
                return DataTypes.ARRAY(toFlinkType(listTypeInfo.getElementTypeInfo()));
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) odpsType;
                return DataTypes.MAP(toFlinkType(mapTypeInfo.getKeyTypeInfo()), toFlinkType(mapTypeInfo.getValueTypeInfo()));
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) odpsType;

                List<String> names = structTypeInfo.getFieldNames();
                List<TypeInfo> typeInfos = structTypeInfo.getFieldTypeInfos();

                DataTypes.Field[] fields = new DataTypes.Field[names.size()];

                for (int i = 0; i < fields.length; i++) {
                    fields[i] = DataTypes.FIELD(names.get(i), toFlinkType(typeInfos.get(i)));
                }
                return DataTypes.ROW(fields);
            default:
                throw new UnsupportedOperationException(
                        String.format("Flink doesn't support Odps data type %s yet.", odpsType.getTypeName()));
        }
    }

    private static class TypeInfoLogicalTypeVisitor extends LogicalTypeDefaultVisitor<TypeInfo> {
        private final DataType dataType;

        public TypeInfoLogicalTypeVisitor(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public TypeInfo visit(CharType charType) {
            if (charType.getLength() > MAX_CHAR_LENGTH) {
                throw new CatalogException(
                        String.format("OdpsCatalog doesn't support char type with length of '%d'. " +
                                        "The maximum length is %d",
                                charType.getLength(), MAX_CHAR_LENGTH));
            }
            return TypeInfoFactory.getCharTypeInfo(charType.getLength());
        }

        @Override
        public TypeInfo visit(VarCharType varCharType) {
            // Flink's StringType is defined as VARCHAR(Integer.MAX_VALUE)
            // We don't have more information in LogicalTypeRoot to distinguish StringType and a VARCHAR(Integer.MAX_VALUE) instance
            // Thus always treat VARCHAR(Integer.MAX_VALUE) as StringType
            if (varCharType.getLength() == Integer.MAX_VALUE) {
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.STRING);
            }
            if (varCharType.getLength() > MAX_VARCHAR_LENGTH) {
                throw new CatalogException(
                        String.format("OdpsCatalog doesn't support varchar type with length of '%d'. " +
                                        "The maximum length is %d",
                                varCharType.getLength(), MAX_VARCHAR_LENGTH));
            }
            return TypeInfoFactory.getVarcharTypeInfo(varCharType.getLength());
        }

        @Override
        public TypeInfo visit(BooleanType booleanType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BOOLEAN);
        }

        @Override
        public TypeInfo visit(VarBinaryType varBinaryType) {
            // Flink's BytesType is defined as VARBINARY(Integer.MAX_VALUE)
            // We don't have more information in LogicalTypeRoot to distinguish BytesType and a VARBINARY(Integer.MAX_VALUE) instance
            // Thus always treat VARBINARY(Integer.MAX_VALUE) as BytesType
            if (varBinaryType.getLength() == VarBinaryType.MAX_LENGTH) {
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BINARY);
            }
            return defaultMethod(varBinaryType);
        }

        @Override
        public TypeInfo visit(DecimalType decimalType) {
            if (decimalType.getPrecision() > MAX_DECIMAL_PRECISION) {
                throw new CatalogException(
                        String.format("OdpsCatalog doesn't support decimal type with precision of '%d'. " +
                                        "The maximum precision is %d",
                                decimalType.getPrecision(), MAX_DECIMAL_PRECISION));
            }
            if (decimalType.getScale() > MAX_DECIMAL_SCALE) {
                throw new CatalogException(
                        String.format("OdpsCatalog doesn't support decimal type with scale of '%d'. " +
                                        "The maximum scale is %d",
                                decimalType.getScale(), MAX_DECIMAL_SCALE));
            }
            return TypeInfoFactory.getDecimalTypeInfo(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public TypeInfo visit(TinyIntType tinyIntType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TINYINT);
        }

        @Override
        public TypeInfo visit(SmallIntType smallIntType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.SMALLINT);
        }

        @Override
        public TypeInfo visit(IntType intType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.INT);
        }

        @Override
        public TypeInfo visit(BigIntType bigIntType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.BIGINT);
        }

        @Override
        public TypeInfo visit(FloatType floatType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.FLOAT);
        }

        @Override
        public TypeInfo visit(DoubleType doubleType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DOUBLE);
        }

        @Override
        public TypeInfo visit(DateType dateType) {
            return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATE);
        }

        @Override
        public TypeInfo visit(TimestampType timestampType) {
            // TODO: in 1.9 always == 10
            // flink is always 3
            if (timestampType.getPrecision() > 3) {
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.TIMESTAMP);
            } else {
                return TypeInfoFactory.getPrimitiveTypeInfo(OdpsType.DATETIME);
            }
        }

        @Override
        public TypeInfo visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            TypeInfo elementTypeInfo = elementType.accept(this);
            if (null != elementTypeInfo) {
                return TypeInfoFactory.getArrayTypeInfo(elementTypeInfo);
            } else {
                return defaultMethod(arrayType);
            }
        }

        @Override
        public TypeInfo visit(MapType mapType) {
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            TypeInfo keyTypeInfo = keyType.accept(this);
            TypeInfo valueTypeInfo = valueType.accept(this);
            if (null == keyTypeInfo || null == valueTypeInfo) {
                return defaultMethod(mapType);
            } else {
                return TypeInfoFactory.getMapTypeInfo(keyTypeInfo, valueTypeInfo);
            }
        }

        @Override
        public TypeInfo visit(RowType rowType) {
            List<String> names = rowType.getFieldNames();
            List<TypeInfo> typeInfos = new ArrayList<>(names.size());
            for (String name : names) {
                TypeInfo typeInfo =
                        rowType.getTypeAt(rowType.getFieldIndex(name)).accept(this);
                if (null != typeInfo) {
                    typeInfos.add(typeInfo);
                } else {
                    return defaultMethod(rowType);
                }
            }
            return TypeInfoFactory.getStructTypeInfo(names, typeInfos);
        }

		@Override
		protected TypeInfo defaultMethod(LogicalType logicalType) {
			throw new UnsupportedOperationException(
					String.format("Flink doesn't support converting type %s to Odps type yet.",
                            dataType.toString()));
		}
	}
}
