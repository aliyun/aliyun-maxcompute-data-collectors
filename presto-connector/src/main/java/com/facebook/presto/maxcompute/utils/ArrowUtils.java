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

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.table.arrow.accessor.ArrowArrayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowBigIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowBitAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDateDayAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowDecimalAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat4Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowFloat8Accessor;
import com.aliyun.odps.table.arrow.accessor.ArrowIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowMapAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowSmallIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowStructAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTimestampAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowTinyIntAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarBinaryAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVarCharAccessor;
import com.aliyun.odps.table.arrow.accessor.ArrowVectorAccessor;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slices;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;

public class ArrowUtils
{
    private static final long MICROS_PER_MILLI = 1_000L;
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    private ArrowUtils() {}

    public static RootAllocator getRootAllocator()
    {
        if (rootAllocator == null) {
            rootAllocator = new RootAllocator(Long.MAX_VALUE);
        }
        return rootAllocator;
    }

    public static ArrowVectorAccessor createColumnVectorAccessor(ValueVector vector, TypeInfo typeInfo)
    {
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return new ArrowBitAccessor((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntAccessor((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntAccessor((SmallIntVector) vector);
            case INT:
                return new ArrowIntAccessor((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntAccessor((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloat4Accessor((Float4Vector) vector);
            case DOUBLE:
                return new ArrowFloat8Accessor((Float8Vector) vector);
            case DECIMAL:
                return new ArrowDecimalAccessor((DecimalVector) vector);
            case STRING:
            case VARCHAR:
            case CHAR:
            case JSON:
                return new ArrowVarCharAccessor((VarCharVector) vector);
            case BINARY:
                return new ArrowVarBinaryAccessor((VarBinaryVector) vector);
            case DATE:
                return new ArrowDateDayAccessor((DateDayVector) vector);
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return new ArrowTimestampAccessor((TimeStampVector) vector);
            case ARRAY:
                return new ArrowArrayAccessorForBlock((ListVector) vector, typeInfo);
            case MAP:
                return new ArrowMapAccessorForRecord((MapVector) vector, typeInfo);
            case STRUCT:
                return new ArrowStructAccessorForRecord((StructVector) vector, typeInfo);
            default:
                throw new UnsupportedOperationException(
                        "Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    public static Object getData(ArrowVectorAccessor dataAccessor, TypeInfo typeInfo, int rowId)
    {
        if (dataAccessor.isNullAt(rowId)) {
            return null;
        }
        switch (typeInfo.getOdpsType()) {
            case BOOLEAN:
                return ((ArrowBitAccessor) dataAccessor).getBoolean(rowId);
            case TINYINT:
                return ((Number) ((ArrowTinyIntAccessor) dataAccessor).getByte(rowId)).longValue();
            case SMALLINT:
                return ((Number) ((ArrowSmallIntAccessor) dataAccessor).getShort(rowId)).longValue();
            case INT:
                return ((Number) ((ArrowIntAccessor) dataAccessor).getInt(rowId)).longValue();
            case BIGINT:
                return ((ArrowBigIntAccessor) dataAccessor).getLong(rowId);
            case FLOAT:
                // return int bits
                return ((Number) Float.floatToIntBits(((ArrowFloat4Accessor) dataAccessor).getFloat(rowId))).longValue();
            case DOUBLE:
                return ((ArrowFloat8Accessor) dataAccessor).getDouble(rowId);
            case DECIMAL:
                BigDecimal decimal = ((ArrowDecimalAccessor) dataAccessor).getDecimal(rowId);
                if (((DecimalTypeInfo) typeInfo).getPrecision() <= MAX_SHORT_PRECISION) {
                    // short decimal (long)
                    return decimal.unscaledValue().longValue();
                }
                else {
                    // long decimal (Slice)
                    return UnscaledDecimal128Arithmetic.unscaledDecimal(decimal.unscaledValue());
                }
            case STRING:
            case VARCHAR:
            case JSON:
                return Slices.wrappedBuffer(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId));
            // char type need to trim tail spaces
            case CHAR:
                String str = new String(((ArrowVarCharAccessor) dataAccessor).getBytes(rowId),
                        StandardCharsets.UTF_8).replaceAll("\\s+$", "");
                return Slices.wrappedBuffer(str.getBytes(StandardCharsets.UTF_8));
            case BINARY:
                return Slices.wrappedBuffer(((ArrowVarBinaryAccessor) dataAccessor).getBinary(rowId));
            case DATE:
                // return epoch days
                return ((Number) ((ArrowDateDayAccessor) dataAccessor).getEpochDay(rowId)).longValue();
            case DATETIME:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                // return epoch millis
                return convertToEpochMillis(((ArrowTimestampAccessor) dataAccessor).getType(),
                        ((ArrowTimestampAccessor) dataAccessor).getEpochTime(rowId));
            case ARRAY:
                return ((ArrowArrayAccessorForBlock) dataAccessor).getArray(rowId);
            case MAP:
                return ((ArrowMapAccessorForRecord) dataAccessor).getMap(rowId);
            case STRUCT:
                return ((ArrowStructAccessorForRecord) dataAccessor).getStruct(rowId);
            case UNKNOWN:
                return ((SimpleDataAccessor) dataAccessor).get(rowId);
            default:
                throw new UnsupportedOperationException(
                        "Datatype not supported: " + typeInfo.getTypeName());
        }
    }

    private static long convertToEpochMillis(ArrowType.Timestamp timestampType, long epochTime)
    {
        switch (timestampType.getUnit()) {
            case SECOND:
                return epochTime * NANOS_PER_MILLI;
            case MILLISECOND:
                return epochTime;
            case MICROSECOND:
                return epochTime / MICROS_PER_MILLI;
            case NANOSECOND:
                return epochTime / NANOS_PER_MILLI;
            default:
                throw new UnsupportedOperationException("Unit not supported: " + timestampType.getUnit());
        }
    }

    public static class ArrowArrayAccessorForBlock
            extends ArrowArrayAccessor<List<Object>>
    {
        private final TypeInfo elementTypeInfo;
        private final ArrowVectorAccessor dataAccessor;

        public ArrowArrayAccessorForBlock(ListVector vector, TypeInfo typeInfo)
        {
            super(vector);
            this.elementTypeInfo = ((ArrayTypeInfo) typeInfo).getElementTypeInfo();

            this.dataAccessor =
                    createColumnVectorAccessor(vector.getDataVector(), elementTypeInfo);
        }

        @Override
        protected List<Object> getArrayData(int offset, int length)
        {
            List<Object> list = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                list.add(getData(dataAccessor, elementTypeInfo, offset + i));
            }
            return list;
        }
    }

    public static class ArrowMapAccessorForRecord
            extends ArrowMapAccessor<Map<Object, Object>>
    {
        private final TypeInfo keyTypeInfo;
        private final TypeInfo valueTypeInfo;
        private final ArrowVectorAccessor keyAccessor;
        private final ArrowVectorAccessor valueAccessor;

        public ArrowMapAccessorForRecord(MapVector mapVector, TypeInfo typeInfo)
        {
            super(mapVector);
            this.keyTypeInfo = ((MapTypeInfo) typeInfo).getKeyTypeInfo();
            this.valueTypeInfo = ((MapTypeInfo) typeInfo).getValueTypeInfo();
            StructVector entries = (StructVector) mapVector.getDataVector();
            this.keyAccessor = createColumnVectorAccessor(
                    entries.getChild(MapVector.KEY_NAME), keyTypeInfo);
            this.valueAccessor = createColumnVectorAccessor(
                    entries.getChild(MapVector.VALUE_NAME), valueTypeInfo);
        }

        @Override
        protected Map<Object, Object> getMapData(int offset, int numElements)
        {
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < numElements; i++) {
                map.put(getData(keyAccessor, keyTypeInfo, offset + i),
                        getData(valueAccessor, valueTypeInfo, offset + i));
            }
            return map;
        }
    }

    public static class ArrowStructAccessorForRecord
            extends ArrowStructAccessor<Struct>
    {
        private final ArrowVectorAccessor[] childAccessors;
        private final TypeInfo structTypeInfo;
        private final List<TypeInfo> childTypeInfos;

        public ArrowStructAccessorForRecord(StructVector structVector,
                TypeInfo typeInfo)
        {
            super(structVector);
            this.structTypeInfo = typeInfo;
            this.childTypeInfos = ((StructTypeInfo) typeInfo).getFieldTypeInfos();
            this.childAccessors = new ArrowVectorAccessor[structVector.size()];
            for (int i = 0; i < childAccessors.length; i++) {
                this.childAccessors[i] = createColumnVectorAccessor(
                        structVector.getVectorById(i), childTypeInfos.get(i));
            }
        }

        @Override
        public Struct getStruct(int rowId)
        {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < childAccessors.length; i++) {
                values.add(getData(childAccessors[i], childTypeInfos.get(i), rowId));
            }
            return new SimpleStruct((StructTypeInfo) structTypeInfo, values);
        }
    }
}
