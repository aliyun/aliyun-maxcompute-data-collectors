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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.cupid.table.v1.vectorized;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.util.Platform;
import com.aliyun.odps.cupid.table.v1.util.Validator;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

import static com.aliyun.odps.cupid.table.v1.util.TableUtils.getTypeInfoFromString;

public final class ColDataVector {

    private Attribute column;
    private byte[] dataBuf;
    private int dataBufSize;
    private byte[] nulls;
    private byte[] deepBuf;

    private final OdpsType odpsType;
    private final TypeInfo odpsTypeInfo;

    private int numRows;
    private int numNulls;
    private int[] binaryOffsets;
    private boolean isOldDecimal;

    public ColDataVector(Attribute column,
                         byte[] dataBuf,
                         int dataBufSize,
                         byte[] nulls,
                         byte[] deepBuf) {
        Validator.checkNotNull(column, "column");
        this.column = column;
        this.dataBuf = dataBuf;
        this.dataBufSize = dataBufSize;
        this.nulls = nulls;
        this.deepBuf = deepBuf;
        this.odpsTypeInfo = getTypeInfoFromString(column.getType());
        this.odpsType = this.odpsTypeInfo.getOdpsType();
        this.numNulls = -1;
        this.binaryOffsets = null;
    }

    public void setDataBufSize(int dataBufSize) {
        this.dataBufSize = dataBufSize;
    }

    public void setDeepBuf(byte[] deepBuf) {
        this.deepBuf = deepBuf;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    public void setBinaryOffsets(int numRows) {
        if (binaryOffsets == null) {
            binaryOffsets = new int[numRows];
            int sum = 0;
            for (int i = 0; i < numRows; i++) {
                binaryOffsets[i] = sum;
                sum += getBinaryLength(i);
            }
        }
    }

    public boolean hasNull() {
        return getNumNulls() > 0;
    }

    public int getNumNulls() {
        if (numNulls < 0) {
            numNulls = 0;
            for (int i = 0; i < numRows; i++) {
                if (isNullAt(i)) {
                    numNulls++;
                }
            }
        }
        return numNulls;
    }

    public byte[] getDeepBuf() {
        return this.deepBuf;
    }

    public byte[] getNulls() {
        return this.nulls;
    }

    public byte[] getDataBuf() {
        return dataBuf;
    }

    public Attribute getColumn() {
        return column;
    }

    public OdpsType getOdpsType() {
        return odpsType;
    }

    public int getDataBufSize() {
        return dataBufSize;
    }

    public int getNumRows() {
        return numRows;
    }

    public boolean isNullAt(int rowId) {
        return nulls[rowId] == 1;
    }

    public boolean getBoolean(int rowId) {
        return Platform.getBoolean(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId);
    }

    public byte getByte(int rowId) {
        return dataBuf[rowId];
    }

    public short getShort(int rowId) {
        return Platform.getShort(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 2L);
    }

    public int getInt(int rowId) {
        return Platform.getInt(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 4L);
    }

    public long getLong(int rowId) {
        return Platform.getLong(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 8L);
    }

    public float getFloat(int rowId) {
        return Platform.getFloat(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 4L);
    }

    public double getDouble(int rowId) {
        return Platform.getDouble(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 8L);
    }

    public BigDecimal getDecimal(int rowId) {
        int precision = ((DecimalTypeInfo) odpsTypeInfo).getPrecision();
        int scale = ((DecimalTypeInfo) odpsTypeInfo).getScale();
        if (isOldDecimal) {
            String decimalStr = getString(rowId);
            try {
                return new BigDecimal(decimalStr);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Failed to get old decimal value: " + decimalStr);
            }
        } else if (precision > 18) {
            byte[] int128Byte = new byte[16];
            for (int i = 0; i < 16; i++) {
                int128Byte[15 - i] = Platform.getByte(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 16 + i);
            }
            BigInteger bigInteger = new BigInteger(int128Byte);
            return new BigDecimal(bigInteger, scale);
        } else if (precision > 9) {
            return new BigDecimal(new BigInteger(String.valueOf(Platform.getLong(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 8))), scale);
        } else if (precision > 4) {
            return new BigDecimal(new BigInteger(String.valueOf(Platform.getLong(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 4))), scale);
        } else {
            return new BigDecimal(new BigInteger(String.valueOf(Platform.getLong(dataBuf, Platform.BYTE_ARRAY_OFFSET + rowId * 2))), scale);
        }
    }

    public String getString(int rowId) {
        byte[] binary = getBinary(rowId);
        return new String(binary, StandardCharsets.UTF_8);
    }

    public byte[] getBinary(int rowId) {
        setBinaryOffsets(this.numRows);
        int offset = binaryOffsets[rowId];
        int numBytes = getBinaryLength(rowId);
        byte[] binary = new byte[numBytes];
        System.arraycopy(deepBuf, offset, binary, 0, numBytes);
        return binary;
    }

    public Timestamp getTimestamp(int rowId) {
        long seconds = Platform.getLong(dataBuf, rowId * 12 + Platform.BYTE_ARRAY_OFFSET);
        int nano = Platform.getInt(dataBuf, rowId * 12 + 8 + Platform.BYTE_ARRAY_OFFSET);
        Timestamp t = new Timestamp(seconds * 1000);
        t.setNanos(nano);
        return t;
    }

    public java.sql.Date getDate(int rowId) {
        return DateUtils.fromDayOffset(getLong(rowId));
    }

    public java.util.Date getDateTime(int rowId) {
        return new java.util.Date(getLong(rowId));
    }

    private int getBinaryLength(int rowId) {
        if (isOldDecimal) {
            return getInt(rowId);
        }
        return (int) getLong(rowId * 2);
    }

    private boolean isStringLikeType(OdpsType odpsType) {
        return odpsType == OdpsType.STRING || odpsType == OdpsType.VARCHAR ||
                odpsType == OdpsType.BINARY || odpsType == OdpsType.CHAR;
    }

    private boolean isOldDecimal(TypeInfo odpsTypeInfo) {
        if (odpsTypeInfo.getOdpsType() == OdpsType.DECIMAL
                && ((DecimalTypeInfo) odpsTypeInfo).getPrecision() == 54
                && ((DecimalTypeInfo) odpsTypeInfo).getScale() == 18) {
            this.isOldDecimal = true;
            return true;
        }
        return false;
    }

    public void close() {
        this.column = null;
        this.dataBuf = null;
        this.dataBufSize = 0;
        this.nulls = null;
        this.deepBuf = null;
    }
}
