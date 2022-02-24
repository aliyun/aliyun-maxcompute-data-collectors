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

package com.aliyun.odps.cupid.table.v1.writer.adaptor;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.util.Platform;
import com.aliyun.odps.cupid.table.v1.util.Validator;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataVector;
import com.aliyun.odps.cupid.table.v1.writer.FileWriter;
import com.aliyun.odps.type.AbstractCharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ColDataRowWriter {

    private FileWriter<ColDataBatch> writer;
    private Column[] cols;

    private int batchSize;
    private int rowCount;

    private Map<Integer, Integer> stringColSizeMap = new HashMap<>(4);
    private byte[][] dataBuf;
    private int[] dataBufSize;
    private byte[][] nulls;
    private byte[][] deepBuf;
    private ColDataVector[] vectors;

    ColDataBatch colDataBatch;

    public ColDataRowWriter(Column[] cols, FileWriter<ColDataBatch> writer) {
        this(cols, writer, 4096);
    }

    public ColDataRowWriter(Column[] cols, FileWriter<ColDataBatch> writer, int batchSize) {
        Validator.checkNotNull(cols, "columns");
        Validator.checkNotNull(writer, "writer");
        Validator.checkInteger(batchSize, 1, "batchSize");
        this.cols = cols;
        this.writer = writer;
        this.batchSize = batchSize;
        this.init();
    }

    public void init() {
        vectors = new ColDataVector[cols.length];
        rowCount = 0;
        dataBuf = new byte[cols.length][];
        dataBufSize = new int[cols.length];
        nulls = new byte[cols.length][];
        deepBuf = new byte[cols.length][];
        initColData();
        colDataBatch = new ColDataBatch(this.vectors);
    }

    public void insert(Row row) throws IOException {
        for (int i = 0; i < dataBuf.length; ++i) {
            TypeInfo odpsTypeInfo = cols[i].getTypeInfo();
            if ((row).isNullAt(i)) {
                nulls[i][rowCount] = 1;
            } else {
                trans(odpsTypeInfo, row, i, rowCount);
                nulls[i][rowCount] = 0;
            }
            dataBufSize[i] += getColumnSize(odpsTypeInfo);
        }
        rowCount++;
        if (rowCount == batchSize) {
            flush();
        }
    }

    public void flush() throws IOException {
        if (rowCount > 0) {
            for (int i = 0; i < dataBuf.length; i++) {
                this.vectors[i].setDataBufSize(dataBufSize[i]);
            }
            colDataBatch.setRowCount(rowCount);
            writer.write(colDataBatch);
            reset();
        }
    }

    private void reset() {
        for (int i = 0; i < dataBufSize.length; ++i) {
            dataBufSize[i] = 0;
            if (stringColSizeMap.containsKey(i)) {
                stringColSizeMap.put(i, 0);
            }
            Platform.setMemory(dataBuf[i], Platform.BYTE_ARRAY_OFFSET, dataBuf[i].length, (byte) 0);
        }
        rowCount = 0;
    }

    public void close() throws IOException {
        try {
            flush();
        } finally {
            writer.close();
        }
    }

    public void commit() throws IOException {
        writer.commit();
    }

    public long getBytesWritten() {
        return writer.getBytesWritten();
    }

    public long getRowsWritten() {
        return writer.getRowsWritten();
    }

    private void initColData() {
        for (int i = 0; i < cols.length; ++i) {
            TypeInfo odpsTypeInfo = cols[i].getTypeInfo();
            dataBuf[i] = new byte[getColumnSize(odpsTypeInfo) * batchSize];
            if (isString(odpsTypeInfo.getOdpsType()) || isOldDecimal(odpsTypeInfo)) {
                stringColSizeMap.put(i, 0);
                deepBuf[i] = new byte[8 * batchSize];
            } else {
                deepBuf[i] = null;
            }
            dataBufSize[i] = 0;
            nulls[i] = new byte[batchSize];
            vectors[i] = new ColDataVector(
                    new Attribute(cols[i].getName(), cols[i].getTypeInfo().getTypeName()),
                    dataBuf[i],
                    0,
                    nulls[i],
                    deepBuf[i]);
        }
    }

    private int getColumnSize(TypeInfo odpsTypeInfo) throws RuntimeException {
        switch (odpsTypeInfo.getOdpsType()) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case FLOAT:
            case INT:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case STRING:
                return 4;
            case DATE:
            case BIGINT:
            case DOUBLE:
            case DATETIME:
                return 8;
            case TIMESTAMP:
                return 12;
            case DECIMAL:
                return getDecimalSize((DecimalTypeInfo) odpsTypeInfo);
            default:
                throw new RuntimeException("Unsupport type: " + odpsTypeInfo.getOdpsType());
        }
    }

    private int getDecimalSize(DecimalTypeInfo decimalInfo) {
        if (decimalInfo.getPrecision() > 38) {
            return 4;
        } else if (decimalInfo.getPrecision() > 18) {
            return 16;
        } else if (decimalInfo.getPrecision() > 9) {
            return 8;
        } else if (decimalInfo.getPrecision() > 4) {
            return 4;
        } else {
            return 2;
        }
    }

    private boolean isString(OdpsType odpsType) {
        return odpsType == OdpsType.STRING || odpsType == OdpsType.VARCHAR ||
                odpsType == OdpsType.BINARY || odpsType == OdpsType.CHAR;
    }

    private boolean isOldDecimal(TypeInfo odpsTypeInfo) {
        return odpsTypeInfo.getOdpsType() == OdpsType.DECIMAL &&
                ((DecimalTypeInfo) odpsTypeInfo).getPrecision() == 54 &&
                ((DecimalTypeInfo) odpsTypeInfo).getScale() == 18;
    }

    private void trans(TypeInfo odpsTypeInfo, Row rec, int ind, int rCnt) throws UnsupportedEncodingException {
        if (isString(odpsTypeInfo.getOdpsType())) {
            byte[] utf8bytes = null;
            if (odpsTypeInfo.getOdpsType() == OdpsType.BINARY) {
                utf8bytes = rec.getBytes(ind);
            } else {
                String utf8String = null;
                switch (odpsTypeInfo.getOdpsType()) {
                    case CHAR:
                        utf8String = rec.getChar(ind).getValue();
                        break;
                    case VARCHAR:
                        utf8String = rec.getVarchar(ind).getValue();
                        break;
                    case STRING:
                        utf8String = rec.getString(ind);
                        break;
                }
                if (odpsTypeInfo.getOdpsType() == OdpsType.CHAR
                        || odpsTypeInfo.getOdpsType() == OdpsType.VARCHAR) {
                    AbstractCharTypeInfo ti = (AbstractCharTypeInfo) odpsTypeInfo;
                    int maxLength = ti.getLength();
                    if (utf8String.length() > maxLength) {
                        utf8String = utf8String.substring(0, maxLength);
                    }
                }
                utf8bytes = utf8String.getBytes("utf-8");
            }
            int numBytes = utf8bytes.length;
            if (stringColSizeMap.get(ind) + numBytes > deepBuf[ind].length) {
                byte[] newArray = new byte[(deepBuf[ind].length + numBytes) * 2];
                System.arraycopy(deepBuf[ind], 0, newArray, 0, stringColSizeMap.get(ind));
                deepBuf[ind] = newArray;
            }
            Platform.copyMemory(utf8bytes, Platform.BYTE_ARRAY_OFFSET,
                    deepBuf[ind], Platform.BYTE_ARRAY_OFFSET + stringColSizeMap.get(ind), numBytes);
            stringColSizeMap.put(ind, stringColSizeMap.get(ind) + numBytes);
            Platform.putInt(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 4, numBytes);
        } else {
            switch (odpsTypeInfo.getOdpsType()) {
                case BOOLEAN:
                    Platform.putBoolean(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt, rec.getBoolean(ind));
                    break;
                case TINYINT:
                    Platform.putByte(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt, rec.getByte(ind));
                    break;
                case SMALLINT:
                    Platform.putShort(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 2, rec.getShort(ind));
                    break;
                case FLOAT:
                    Platform.putFloat(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 4, rec.getFloat(ind));
                    break;
                case INT:
                    Platform.putInt(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 4, rec.getInt(ind));
                    break;
                case DATE:
                    Platform.putLong(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 8, DateUtils.getDayOffset(rec.getDate(ind)));
                    break;
                case DATETIME:
                    Platform.putLong(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 8, DateUtils.date2ms(rec.getDatetime(ind)));
                    break;
                case TIMESTAMP:
                    Timestamp timeStamp = rec.getTimeStamp(ind);
                    int nanoSeconds = timeStamp.getNanos();
                    long seconds = timeStamp.getTime() - (nanoSeconds / 1000000) / 1000;
                    Platform.putLong(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 12, seconds);
                    Platform.putInt(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 12 + 8, nanoSeconds);
                    break;
                case BIGINT:
                    Platform.putLong(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 8, rec.getLong(ind));
                    break;
                case DOUBLE:
                    Platform.putDouble(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 8, rec.getDouble(ind));
                    break;
                case DECIMAL:
                    DecimalTypeInfo decimalInfo = (DecimalTypeInfo) odpsTypeInfo;
                    BigDecimal decimal = rec.getDecimal(ind);
                    if (decimalInfo.getPrecision() > 38) {
                        byte[] decimalBytes = decimal.toString().getBytes();
                        int numBytes = decimalBytes.length;
                        if (stringColSizeMap.get(ind) + numBytes > deepBuf[ind].length) {
                            byte[] newArray = new byte[(deepBuf[ind].length + numBytes) * 2];
                            System.arraycopy(deepBuf[ind], 0, newArray, 0, stringColSizeMap.get(ind));
                            deepBuf[ind] = newArray;
                        }
                        System.arraycopy(decimalBytes, 0, deepBuf[ind], stringColSizeMap.get(ind), numBytes);
                        stringColSizeMap.put(ind, stringColSizeMap.get(ind) + numBytes);
                        Platform.putInt(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 4, numBytes);
                    } else if (decimalInfo.getPrecision() > 18) {
                        byte[] byteArray = decimal.unscaledValue().toByteArray();
                        int length = byteArray.length;
                        for (int i = 0; i < length; i++) {
                            Platform.putByte(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 16 + i, byteArray[length - i - 1]);
                        }
                        if (decimal.signum() == -1) {
                            for (int i = length; i < 16; i++) {
                                Platform.putByte(dataBuf[ind], Platform.BYTE_ARRAY_OFFSET + rCnt * 16 + i, (byte) -1);
                            }
                        }
                    } else if (decimalInfo.getPrecision() > 9) {
                        Platform.putLong(dataBuf[ind],
                                Platform.BYTE_ARRAY_OFFSET + rCnt * 8, decimal.unscaledValue().longValue());
                    } else if (decimalInfo.getPrecision() > 4) {
                        Platform.putInt(dataBuf[ind],
                                Platform.BYTE_ARRAY_OFFSET + rCnt * 4, decimal.unscaledValue().intValue());
                    } else {
                        Platform.putShort(dataBuf[ind],
                                Platform.BYTE_ARRAY_OFFSET + rCnt * 2, decimal.unscaledValue().shortValue());
                    }
                    break;
                default:
                    throw new RuntimeException("not supported type " + odpsTypeInfo.getOdpsType());
            }
        }
    }
}
