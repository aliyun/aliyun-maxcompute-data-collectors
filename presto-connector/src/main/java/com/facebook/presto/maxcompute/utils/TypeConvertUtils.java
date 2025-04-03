/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.maxcompute.utils;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.maxcompute.MaxComputeErrorCode;
import com.facebook.presto.spi.PrestoException;

public class TypeConvertUtils
{
    private TypeConvertUtils() {}

    /**
     * Convert MaxCompute data type to Presto data type.
     * <p>
     * This method is responsible for mapping the {@link TypeInfo} type of MaxCompute to the {@link Type} type of Presto.
     * Note: This method currently does not support complex types (such as arrays, maps, and structures).
     *
     * @param odpsType MaxCompute data type information
     * @return corresponding Presto data type
     * @throws PrestoException if an unsupported data type is encountered
     * <p>
     * The supported MaxCompute to Presto type mapping is as follows:
     * <ul>
     * <li>TINYINT -> {@link TinyintType#TINYINT}</li>
     * <li>SMALLINT -> {@link SmallintType#SMALLINT}</li>
     * <li>INT -> {@link IntegerType#INTEGER}</li>
     * <li>BIGINT -> {@link BigintType#BIGINT}</li>
     * <li>CHAR -> {@link CharType} created based on character length</li>
     * <li>VARCHAR -> {@link VarcharType} created based on character length</li>
     * <li>STRING, JSON -> {@link VarcharType#VARCHAR}</li>
     * <li>BINARY -> {@link VarbinaryType#VARBINARY}</li>
     * <li>DATE -> {@link DateType#DATE}</li>
     * <li>TIMESTAMP, TIMESTAMP_NTZ, DATETIME -> {@link TimestampType#TIMESTAMP}</li>
     * <li>FLOAT -> {@link RealType#REAL}</li>
     * <li>DOUBLE -> {@link DoubleType#DOUBLE}</li>
     * <li>DECIMAL -> {@link DecimalType} created based on precision and scale</li>
     * <li>BOOLEAN -> {@link BooleanType#BOOLEAN}</li>
     * </ul>
     * If MaxCompute encounters an unsupported type, it will throw {@link PrestoException}.
     */
    public static Type toPrestoType(TypeInfo odpsType)
    {
        // TODO: support complex type (array, map, struct)
        switch (odpsType.getOdpsType()) {
            case TINYINT:
                // long
                return TinyintType.TINYINT;
            case SMALLINT:
                // long
                return SmallintType.SMALLINT;
            case INT:
                // long
                return IntegerType.INTEGER;
            case BIGINT:
                // long
                return BigintType.BIGINT;
            case CHAR:
                // Slice
                return CharType.createCharType(((CharTypeInfo) odpsType).getLength());
            case VARCHAR:
                // Slice
                return VarcharType.createVarcharType(((VarcharTypeInfo) odpsType).getLength());
            case STRING:
            case JSON:
                // Slice
                return VarcharType.VARCHAR;
            case BINARY:
                // Slice
                return VarbinaryType.VARBINARY;
            case DATE:
                // long
                return DateType.DATE;
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
            case DATETIME:
                // long
                return TimestampType.TIMESTAMP;
            case FLOAT:
                // long
                return RealType.REAL;
            case DOUBLE:
                // double
                return DoubleType.DOUBLE;
            case DECIMAL:
                // long or Slice
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) odpsType;
                int precision = decimalTypeInfo.getPrecision();
                int scale = decimalTypeInfo.getScale();
                if (decimalTypeInfo.getPrecision() > Decimals.MAX_PRECISION) {
                    precision = Decimals.MAX_PRECISION;
                }
                return DecimalType.createDecimalType(precision, scale);
            case BOOLEAN:
                // boolean
                return BooleanType.BOOLEAN;
            case ARRAY:
                // Block
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) odpsType;
                return new ArrayType(toPrestoType(arrayTypeInfo.getElementTypeInfo()));
            case MAP:
            case STRUCT:
                // TODO: support map and struct
            default:
                throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "unsupported type: " + odpsType.getTypeName());
        }
    }
}
