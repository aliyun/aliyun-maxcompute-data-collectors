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
package io.trino.plugin.maxcompute.utils;

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.plugin.maxcompute.MaxComputeErrorCode;
import io.trino.spi.TrinoException;

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
     * @throws TrinoException if an unsupported data type is encountered
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
     * If MaxCompute encounters an unsupported type, it will throw {@link TrinoException}.
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
                // long TODO
                return TimestampType.TIMESTAMP_NANOS;
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
                throw new TrinoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "unsupported type: " + odpsType.getTypeName());
        }
    }
}
