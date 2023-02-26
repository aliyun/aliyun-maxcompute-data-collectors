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

import com.aliyun.odps.PartitionSpec;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.odps.util.RowDataToOdpsConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PartitionComputer implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String defaultPartValue;
    private final List<String> partitionColumns;
    private final int[] partitionIndexes;
    private final Map<String, String> staticPartitionSpec;

    public PartitionComputer(RowType rowType,
							 List<String> partitionColumns,
							 String staticPartition,
							 String defaultPartValue) {
        this.staticPartitionSpec = new LinkedHashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(staticPartition)) {
            PartitionSpec partitionSpec = new PartitionSpec(staticPartition);
            partitionSpec.keys().forEach((key) -> {
                staticPartitionSpec.put(key, partitionSpec.get(key));
            });
        }
        this.partitionColumns = partitionColumns.stream()
                .filter(col -> !staticPartitionSpec.containsKey(col))
                .collect(Collectors.toList());
		this.defaultPartValue = defaultPartValue;
		List<String> columnNames = rowType.getFields().stream()
				.map(RowType.RowField::getName).collect(Collectors.toList());
		this.partitionIndexes = this.partitionColumns.stream()
                .mapToInt(columnNames::indexOf).toArray();

		// TODO: For
		final RowDataToOdpsConverters.RowDataToOdpsFieldConverter[] fieldConverters =
				rowType.getChildren().stream()
						.map(RowDataToOdpsConverters::createConverter)
						.toArray(RowDataToOdpsConverters.RowDataToOdpsFieldConverter[]::new);

		final LogicalType[] fieldTypes =
				rowType.getFields().stream()
						.map(RowType.RowField::getType)
						.toArray(LogicalType[]::new);
		final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
		}
    }

    public LinkedHashMap<String, String> generatePartValues(RowData in, PartitionAssigner.Context context) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>(staticPartitionSpec);
        for (int i = 0; i < partitionIndexes.length; i++) {
            int index = partitionIndexes[i];
            if (index == -1) {
                throw new FlinkOdpsException("Invalid partition columns:" + this.partitionColumns);
            }
            Object field = null;
            String partitionValue = field != null ? field.toString() : null;
            if (StringUtils.isNullOrWhitespaceOnly(partitionValue)) {
                partitionValue = defaultPartValue;
            }
            partSpec.put(partitionColumns.get(i), partitionValue);
        }
        return partSpec;
    }
}