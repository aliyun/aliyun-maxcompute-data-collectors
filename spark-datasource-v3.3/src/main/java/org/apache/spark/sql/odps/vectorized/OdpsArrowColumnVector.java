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

package org.apache.spark.sql.odps.vectorized;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.table.arrow.accessor.*;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.spark.sql.execution.datasources.v2.odps.ArrowUtils;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.sql.odps.table.utils.DateTimeConstants.NANOS_PER_MICROS;
import static org.apache.spark.sql.odps.table.utils.DateTimeConstants.MICROS_PER_MILLIS;
import static org.apache.arrow.vector.types.TimeUnit.MICROSECOND;

public class OdpsArrowColumnVector extends ColumnVector {
    private final ArrowVectorAccessor accessor;
    private OdpsArrowColumnVector[] childColumns;
    private final boolean isTimestamp;
    private ArrowType.Timestamp timestampType;
    private final boolean isDate;
    private boolean isDatetimeMilliMode = false;
    private boolean shouldTransform = false;

    @Override
    public boolean hasNull() {
        return accessor.getNullCount() > 0;
    }

    @Override
    public int numNulls() {
        return accessor.getNullCount();
    }

    @Override
    public void close() {
        if (childColumns != null) {
            for (int i = 0; i < childColumns.length; i++) {
                childColumns[i].close();
                childColumns[i] = null;
            }
            childColumns = null;
        }
        accessor.close();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return accessor.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return ((ArrowBitAccessor) accessor).getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return ((ArrowTinyIntAccessor) accessor).getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return ((ArrowSmallIntAccessor) accessor).getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        if (isDate) {
            /**
             * TODO: rebaseJulianToGregorianDays
             * return RebaseDateTime.rebaseJulianToGregorianDays(((ArrowDateDayAccessor) accessor).getEpochDay(rowId));
             */
            return ((ArrowDateDayAccessor) accessor).getEpochDay(rowId);
        } else {
            return ((ArrowIntAccessor) accessor).getInt(rowId);
        }
    }

    @Override
    public long getLong(int rowId) {
        if (isTimestamp) {
            if (shouldTransform) {
                if (isDatetimeMilliMode) {
                    return Math.multiplyExact(((ArrowDateMilliAccessor) accessor).getEpochTime(rowId),
                            MICROS_PER_MILLIS);
                } else {
                    return parseEpochTimeToMicroUnit(((ArrowTimestampAccessor) accessor).getEpochTime(rowId));
                }
            } else {
                return ((ArrowTimestampAccessor) accessor).getEpochTime(rowId);
            }
        } else {
            return ((ArrowBigIntAccessor) accessor).getLong(rowId);
        }
    }

    @Override
    public float getFloat(int rowId) {
        return ((ArrowFloat4Accessor) accessor).getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return ((ArrowFloat8Accessor) accessor).getDouble(rowId);
    }

    @Override
    public Decimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) return null;
        return Decimal.apply(((ArrowDecimalAccessor) accessor).getDecimal(rowId), precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int rowId) {
        if (isNullAt(rowId)) return null;
        return ((StringAccessor) accessor).getUTF8String(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        if (isNullAt(rowId)) return null;
        return ((ArrowVarBinaryAccessor) accessor).getBinary(rowId);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        if (isNullAt(rowId)) return null;
        return ((ArrayAccessor) accessor).getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        if (isNullAt(rowId)) return null;
        return ((MapAccessor) accessor).getMap(rowId);
    }

    @Override
    public OdpsArrowColumnVector getChild(int ordinal) {
        return childColumns[ordinal];
    }

    public OdpsArrowColumnVector(ValueVector vector, TypeInfo typeInfo) {
        super(ArrowUtils.fromArrowField(vector.getField()));
        isTimestamp = type instanceof TimestampType;
        isDate = type instanceof DateType;

        if (vector instanceof BitVector) {
            accessor = new ArrowBitAccessor((BitVector) vector);
        } else if (vector instanceof TinyIntVector) {
            accessor = new ArrowTinyIntAccessor((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            accessor = new ArrowSmallIntAccessor((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            accessor = new ArrowIntAccessor((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            accessor = new ArrowBigIntAccessor((BigIntVector) vector);
        } else if (vector instanceof Float4Vector) {
            accessor = new ArrowFloat4Accessor((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            accessor = new ArrowFloat8Accessor((Float8Vector) vector);
        } else if (vector instanceof DecimalVector) {
            accessor = new ArrowDecimalAccessor((DecimalVector) vector);
        } else if (vector instanceof VarCharVector) {
            accessor = new StringAccessor((VarCharVector) vector, typeInfo.getOdpsType().equals(OdpsType.CHAR));
        } else if (vector instanceof VarBinaryVector) {
            accessor = new ArrowVarBinaryAccessor((VarBinaryVector) vector);
        } else if (vector instanceof DateDayVector) {
            accessor = new ArrowDateDayAccessor((DateDayVector) vector);
        } else if (vector instanceof DateMilliVector) {
            accessor = new ArrowDateMilliAccessor((DateMilliVector) vector);
            shouldTransform = true;
            isDatetimeMilliMode = true;
        } else if (vector instanceof TimeStampVector) {
            accessor = new ArrowTimestampAccessor((TimeStampVector) vector);
            timestampType = ((ArrowTimestampAccessor) accessor).getType();
            if (!timestampType.getUnit().equals(MICROSECOND)) {
                shouldTransform = true;
            }
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            accessor = new MapAccessor(mapVector, (MapTypeInfo) typeInfo);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            accessor = new ArrayAccessor(listVector, (ArrayTypeInfo) typeInfo);
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            accessor = new StructAccessor(structVector);

            childColumns = new OdpsArrowColumnVector[structVector.size()];
            for (int i = 0; i < childColumns.length; ++i) {
                childColumns[i] = new OdpsArrowColumnVector(structVector.getVectorById(i),
                        ((StructTypeInfo)typeInfo).getFieldTypeInfos().get(i));
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private long parseEpochTimeToMicroUnit(long epochTime) {
        if (timestampType.getUnit() == TimeUnit.NANOSECOND) {
            return Math.floorDiv(epochTime, NANOS_PER_MICROS);
        }
        if (timestampType.getUnit() == TimeUnit.MILLISECOND) {
            return Math.multiplyExact(epochTime, MICROS_PER_MILLIS);
        }
        throw new UnsupportedOperationException(
                "Unit not supported: " + timestampType.getUnit());
    }

    private static class StringAccessor extends ArrowVarCharAccessor {

        private final boolean isChar;

        StringAccessor(VarCharVector vector, boolean isChar) {
            super(vector);
            this.isChar = isChar;
        }

        final UTF8String getUTF8String(int rowId) {
            this.getStringResult(rowId);
            if (stringResult.isSet == 0) {
                return null;
            } else {
                UTF8String result = UTF8String.fromAddress(null,
                        stringResult.buffer.memoryAddress() + stringResult.start,
                        stringResult.end - stringResult.start);
                if (isChar) {
                    return result.trimRight();
                }
                return result;
            }
        }
    }

    private static class ArrayAccessor extends ArrowArrayAccessor<ColumnarArray> {

        private final OdpsArrowColumnVector arrayData;

        ArrayAccessor(ListVector vector, ArrayTypeInfo arrayTypeInfo) {
            super(vector);
            this.arrayData = new OdpsArrowColumnVector(vector.getDataVector(),
                    (arrayTypeInfo).getElementTypeInfo());
        }

        @Override
        protected ColumnarArray getArrayData(int offset, int numElements) {
            return new ColumnarArray(arrayData, offset, numElements);
        }
    }

    private static class MapAccessor extends ArrowMapAccessor<ColumnarMap> {
        private final OdpsArrowColumnVector keys;
        private final OdpsArrowColumnVector values;

        MapAccessor(MapVector vector, MapTypeInfo mapTypeInfo) {
            super(vector);
            StructVector entries = (StructVector) vector.getDataVector();
            this.keys = new OdpsArrowColumnVector(entries.getChild(MapVector.KEY_NAME),
                    mapTypeInfo.getKeyTypeInfo());
            this.values = new OdpsArrowColumnVector(entries.getChild(MapVector.VALUE_NAME),
                    mapTypeInfo.getValueTypeInfo());
        }

        @Override
        protected ColumnarMap getMapData(int offset, int numElements) {
            return new ColumnarMap(keys, values, offset, numElements);
        }
    }

    private static class StructAccessor extends ArrowVectorAccessor {
        StructAccessor(StructVector vector) {
            super(vector);
        }
    }
}