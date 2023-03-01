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

package org.apache.flink.odps.sink.partition;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.sink.utils.RowDataProjection;
import org.apache.flink.odps.util.OdpsTableUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.odps.sink.utils.RowDataProjection.getProjection;
import static org.apache.flink.odps.util.OdpsUtils.objToString;

public class RecordKeySelector implements KeySelector<RowData, String> {

    private static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";
    private final boolean hasRecordKey;
    private String[] recordKeyFields;
    private boolean simpleRecordKey = false;
    private final RowDataProjection recordKeyProjection;
    private RowData.FieldGetter recordKeyFieldGetter;

    public RecordKeySelector(TableSchema tableSchema,
                             List<String> primaryKeys) {
        List<Column> columnList = new ArrayList<>();
        columnList.addAll(tableSchema.getColumns());
        columnList.addAll(tableSchema.getPartitionColumns());
        // TODO: set row type
        RowType rowType =
                ((RowType) OdpsTableUtil.toRowDataType(columnList).getLogicalType());
        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = rowType.getChildren();

        this.hasRecordKey = primaryKeys != null && !primaryKeys.isEmpty();
        if (!hasRecordKey) {
            this.recordKeyProjection = null;
        } else {
            if (primaryKeys.size() == 1) {
                this.simpleRecordKey = true;
                int recordKeyIdx = fieldNames.indexOf(primaryKeys.get(0));
                this.recordKeyFieldGetter = RowData.createFieldGetter(fieldTypes.get(recordKeyIdx), recordKeyIdx);
                this.recordKeyProjection = null;
            } else {
                this.recordKeyProjection = getProjection(primaryKeys, fieldNames, fieldTypes);
            }
            this.recordKeyFields = primaryKeys.toArray(new String[0]);
        }
    }

    @Override
    public String getKey(RowData rowData) {
        if (!hasRecordKey) {
            return EMPTY_RECORDKEY_PLACEHOLDER;
        } else if (this.simpleRecordKey) {
            return getRecordKey(recordKeyFieldGetter.getFieldOrNull(rowData), this.recordKeyFields[0]);
        } else {
            Object[] keyValues = this.recordKeyProjection.projectAsValues(rowData);
            return getRecordKey(keyValues, this.recordKeyFields);
        }
    }

    private static String getRecordKey(Object recordKeyValue, String recordKeyField) {
        String recordKey = objToString(recordKeyValue);
        if (recordKey == null) {
            throw new FlinkOdpsException("recordKey value for field: \"" + recordKeyField + "\" cannot be null or empty.");
        }
        return recordKey;
    }

    private static String getRecordKey(Object[] keyValues, String[] keyFields) {
        StringBuilder recordKey = new StringBuilder();
        for (int i = 0; i < keyValues.length; i++) {
            String recordKeyField = keyFields[i];
            Object value = keyValues[i];
            String recordKeyValue = objToString(value);
            if (recordKeyValue == null) {
                throw new FlinkOdpsException("recordKey values: \"" + recordKey + "\" for fields: "
                        + Arrays.toString(keyFields) + " cannot be null.");
            } else if (recordKeyValue.isEmpty()) {
                recordKey.append(recordKeyField).append(":").append(EMPTY_RECORDKEY_PLACEHOLDER).append(",");
            } else {
                recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
            }
        }
        recordKey.deleteCharAt(recordKey.length() - 1);
        return recordKey.toString();
    }
}
