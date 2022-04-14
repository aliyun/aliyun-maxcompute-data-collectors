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
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataBatch;
import com.aliyun.odps.cupid.table.v1.vectorized.ColDataVector;
import com.aliyun.odps.data.*;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

public class MutableColumnarRow {

    public int rowId;
    private final Record row;
    private final Column[] fullColumns;
    private final Map<String, Object> partitionColumns;
    private final Map<String, Integer> primaryColumnPos;
    private ColDataVector[] odpsColumnVectors;

    public MutableColumnarRow(Column[] fullColumns, Map<String, String> partitionSpec) {
        this.row = new ArrayRecord(fullColumns);
        this.fullColumns = fullColumns;
        this.partitionColumns = new HashMap<>(partitionSpec.size());
        this.primaryColumnPos = new HashMap<>();
        int columnCount = 0;
        for (Column column : fullColumns) {
            if (!partitionSpec.containsKey(column.getName())) {
                primaryColumnPos.put(column.getName(), columnCount);
                columnCount++;
            } else {
                partitionColumns.put(column.getName(), OdpsUtils.convertPartitionColumn(partitionSpec.get(column.getName()), column.getTypeInfo()));
            }
        }
    }

    public void updateColDataBatch(ColDataBatch colDataBatch) {
        if (colDataBatch.getColumnCount() < fullColumns.length) {
            Preconditions.checkArgument(partitionColumns.size() + colDataBatch.getColumnCount() == fullColumns.length);
        }
        this.odpsColumnVectors = new ColDataVector[colDataBatch.getColumnCount()];
        for (Column fullColumn : fullColumns) {
            String colName = fullColumn.getName();
            if (primaryColumnPos.containsKey(colName)) {
                int pos = primaryColumnPos.get(colName);
                this.odpsColumnVectors[pos] = colDataBatch.getVectors()[pos];
            }
        }
    }

    public Record getRow() {
        for (int i = 0; i < fullColumns.length; i++) {
            String colName = fullColumns[i].getName();
            if (partitionColumns.containsKey(colName)) {
                this.row.set(i, partitionColumns.get(colName));
            } else {
                this.row.set(i, getField(primaryColumnPos.get(colName)));
            }
        }
        return this.row;
    }

    public Object getField(int pos) {
        if (this.odpsColumnVectors[pos].isNullAt(rowId)) {
            return null;
        }
        OdpsType odpsType = this.odpsColumnVectors[pos].getOdpsType();
        switch (odpsType) {
            case INT:
                return this.odpsColumnVectors[pos].getInt(rowId);
            case TINYINT:
                return this.odpsColumnVectors[pos].getByte(rowId);
            case SMALLINT:
                return this.odpsColumnVectors[pos].getShort(rowId);
            case BIGINT:
                return this.odpsColumnVectors[pos].getLong(rowId);
            case BOOLEAN:
                return this.odpsColumnVectors[pos].getBoolean(rowId);
            case FLOAT:
                return this.odpsColumnVectors[pos].getFloat(rowId);
            case DOUBLE:
                return this.odpsColumnVectors[pos].getDouble(rowId);
            case CHAR:
                return new Char(this.odpsColumnVectors[pos].getString(rowId));
            case VARCHAR:
                return new Varchar(this.odpsColumnVectors[pos].getString(rowId));
            case STRING:
                return this.odpsColumnVectors[pos].getString(rowId);
            case DECIMAL:
                return this.odpsColumnVectors[pos].getDecimal(rowId);
            case DATE:
                return this.odpsColumnVectors[pos].getDate(rowId);
            case DATETIME:
                return this.odpsColumnVectors[pos].getDateTime(rowId);
            case TIMESTAMP:
                return this.odpsColumnVectors[pos].getTimestamp(rowId);
            case BINARY:
                return new Binary(this.odpsColumnVectors[pos].getBinary(rowId));
            default:
                throw new RuntimeException("Unsupported Type: " + odpsType);
        }
    }
}
