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

package org.apache.flink.odps.vectorized;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.TypeInfo;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.odps.util.OdpsTypeConverter;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class ColDataRowImpl<T> implements com.aliyun.odps.cupid.table.v1.writer.adaptor.Row {

    private TypeComparator<T> typeComparator;
    private T row;
    private Object[] sourceValues;
    private OdpsUtils.RecordType recordType = null;
    private final Column[] columns;
    private final TypeInfo[] odpsTypeInfo;
    private final OdpsTypeConverter[] odpsTypeConverter;

    public ColDataRowImpl(Column[] columns) {
        this.columns = columns;
        this.sourceValues = new Object[columns.length];
        this.odpsTypeConverter = new OdpsTypeConverter[columns.length];
        this.odpsTypeInfo = new TypeInfo[columns.length];
        for (int i = 0; i < columns.length; i++) {
            odpsTypeInfo[i] = columns[i].getTypeInfo();
            odpsTypeConverter[i] = OdpsTypeConverter.valueOf(odpsTypeInfo[i].getOdpsType().name());
        }
    }

    public void setRow(T row) {
        if (recordType == null) {
            if (row instanceof Row) {
                this.recordType = OdpsUtils.RecordType.FLINK_ROW;
            } else if (row instanceof Tuple) {
                this.recordType = OdpsUtils.RecordType.FLINK_TUPLE;
            } else {
                this.recordType = OdpsUtils.RecordType.POJO;
                this.typeComparator = OdpsUtils.buildTypeComparator(row, Arrays.stream(columns).map(Column::getName).collect(Collectors.toList()));
            }
        }
        this.row = row;
        if (this.recordType == OdpsUtils.RecordType.POJO) {
            buildFlinkRow(row);
        }
    }

    private void buildFlinkRow(T row) {
        Object[] keyArray = new Object[this.columns.length];
        typeComparator.extractKeys(row, keyArray, 0);
        sourceValues = keyArray;
    }

    private Object getSourceValue(int idx) {
        switch (recordType) {
            case FLINK_ROW:
                return ((Row) this.row).getField(idx);
            case FLINK_TUPLE:
                return ((Tuple) this.row).getField(idx);
            default:
                return sourceValues[idx];
        }
    }

    @Override
    public boolean isNullAt(int idx) {
        switch (recordType) {
            case FLINK_ROW:
                return ((Row) this.row).getField(idx) == null;
            case FLINK_TUPLE:
                return ((Tuple) this.row).getField(idx) == null;
            default:
                return sourceValues[idx] == null;
        }
    }

    @Override
    public boolean getBoolean(int idx) {
        return (boolean) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public byte getByte(int idx) {
        return (byte) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public short getShort(int idx) {
        return (short) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public int getInt(int idx) {
        return (int) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public long getLong(int idx) {
        return (long) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public float getFloat(int idx) {
        return (float) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public double getDouble(int idx) {
        return (double) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public Date getDatetime(int idx) {
        return (Date) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public java.sql.Date getDate(int idx) {
        return (java.sql.Date) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public Timestamp getTimeStamp(int idx) {
        return (Timestamp) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public BigDecimal getDecimal(int idx) {
        return (BigDecimal) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public String getString(int idx) {
        return (String) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public Char getChar(int idx) {
        return (Char) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public Varchar getVarchar(int idx) {
        return (Varchar) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }

    @Override
    public byte[] getBytes(int idx) {
        return (byte[]) odpsTypeConverter[idx].toOdpsField(getSourceValue(idx), odpsTypeInfo[idx]);
    }
}
