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

package org.apache.flink.odps.output.stream;

import com.aliyun.odps.PartitionSpec;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PartitionComputer<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String defaultPartValue;
	private final List<String> partitionColumns;
	private final int[] partitionIndexes;
	private final Map<String, String> staticPartitionSpec;
	private final Object[] keyArray;

	private TypeComparator<T> typeComparator;

	public PartitionComputer(String defaultPartValue,
							 List<String> columnNames,
							 List<String> partitionColumns,
							 String staticPartition) {
		this.defaultPartValue = defaultPartValue;
		this.staticPartitionSpec = new LinkedHashMap<>();
		if (!StringUtils.isNullOrWhitespaceOnly(staticPartition)) {
			PartitionSpec partitionSpec = new PartitionSpec(staticPartition);
			partitionSpec.keys().forEach((key)->{
				staticPartitionSpec.put(key, partitionSpec.get(key));
			});
		}
		this.partitionColumns = partitionColumns.stream()
				.filter(col ->!staticPartitionSpec.containsKey(col))
				.collect(Collectors.toList());
		this.partitionIndexes = this.partitionColumns.stream()
				.mapToInt(columnNames::indexOf).toArray();
		this.keyArray = new Object[partitionIndexes.length];
	}

	public LinkedHashMap<String, String> generatePartValues(T in, PartitionAssigner.Context context) {
		LinkedHashMap<String, String> partSpec = new LinkedHashMap<>(staticPartitionSpec);
		for (int i = 0; i < partitionIndexes.length; i++) {
			int index = partitionIndexes[i];
			if (index == -1) {
				throw new FlinkOdpsException("Invalid partition columns:" + this.partitionColumns);
			}
			Object field = null;
			if (in instanceof Row) {
				field = ((Row)in).getField(index);
			} else if (in instanceof Tuple) {
				field = ((Tuple)in).getField(index);
			} else {
				if (typeComparator == null) {
					this.typeComparator = OdpsUtils.buildTypeComparator(in, partitionColumns);
				}
				if (i == 0) {
					typeComparator.extractKeys(in, keyArray, 0);
				}
				field = keyArray[i];
			}
			String partitionValue = field != null ? field.toString() : null;
			if (StringUtils.isNullOrWhitespaceOnly(partitionValue)) {
				partitionValue = defaultPartValue;
			}
			partSpec.put(partitionColumns.get(i), partitionValue);
		}
		return partSpec;
	}
}