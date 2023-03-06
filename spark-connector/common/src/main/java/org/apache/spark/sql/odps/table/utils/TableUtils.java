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

package org.apache.spark.sql.odps.table.utils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.enviroment.ExecutionEnvironment;
import com.aliyun.odps.table.utils.Preconditions;
import com.aliyun.odps.tunnel.TableTunnel;

import java.util.*;
import java.util.stream.Collectors;

public class TableUtils {

    public static PartitionSpec toOdpsPartitionSpec(Map<String, String> partitionSpec) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return new PartitionSpec();
        }
        PartitionSpec odpsPartitionSpec = new PartitionSpec();
        partitionSpec.forEach(odpsPartitionSpec::set);
        return odpsPartitionSpec;
    }

    public static LinkedHashMap<String, String> toPartitionSpecMap(PartitionSpec partitionSpec) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return new LinkedHashMap<>();
        }
        LinkedHashMap<String, String> spec = new LinkedHashMap<>(partitionSpec.keys().size());
        partitionSpec.keys().forEach(key -> spec.put(key, partitionSpec.get(key)));
        return spec;
    }

    public static void validatePartitionSpec(PartitionSpec staticPartitionSpec,
                                             List<String> partitionKeys) {
        List<String> unknownPartCols = staticPartitionSpec
                .keys()
                .stream()
                .filter(k -> !partitionKeys.contains(k))
                .collect(Collectors.toList());
        Preconditions.checkArgument(
                unknownPartCols.isEmpty(),
                "Static partition spec contains unknown partition column: " + unknownPartCols.toString());
        int numStaticPart = staticPartitionSpec.keys().size();
        if (numStaticPart < partitionKeys.size()) {
            for (String partitionCol : partitionKeys) {
                if (!staticPartitionSpec.keys().contains(partitionCol)) {
                    Preconditions.checkArgument(numStaticPart == 0,
                            "Dynamic partition cannot appear before static partition");
                    return;
                } else {
                    numStaticPart--;
                }
            }
        }
    }

    public static void validateRequiredDataColumns(List<String> requiredDataColumns,
                                                   List<Column> dataColumns) {
        Set<String> dataColumnsSet = dataColumns.stream()
                .map(Column::getName)
                .collect(Collectors.toSet());
        List<String> unknownDataCols = requiredDataColumns
                .stream()
                .filter(k -> !dataColumnsSet.contains(k.toLowerCase()))
                .collect(Collectors.toList());
        Preconditions.checkArgument(unknownDataCols.isEmpty(),
                "The specified columns is not valid");
    }

    public static void validateRequiredPartitionColumns(List<String> requiredPartitionColumns,
                                                        List<Column> partitionColumns) {
        List<String> partitionKeys = partitionColumns.stream()
                .map(Column::getName)
                .collect(Collectors.toList());
        Set<String> partitionColumnSet = new HashSet<>(partitionKeys);
        List<String> unknownPartCols = requiredPartitionColumns
                .stream()
                .filter(k -> !partitionColumnSet.contains(k.toLowerCase()))
                .collect(Collectors.toList());
        Preconditions.checkArgument(unknownPartCols.isEmpty(),
                "The specified columns is not valid");

        int requiredPartCols = requiredPartitionColumns.size();
        int i = 0;
        int pos = 0;
        while (i < requiredPartCols) {
            String requiredPartitionCol = requiredPartitionColumns.get(i).toLowerCase();
            String partitionCol = partitionKeys.get(pos);
            while (requiredPartitionCol.equals(partitionCol)) {
                i++;
                if (i < requiredPartCols) {
                    requiredPartitionCol = requiredPartitionColumns.get(i).toLowerCase();
                } else {
                    return;
                }
            }
            pos++;
            Preconditions.checkArgument(
                    pos < partitionKeys.size(),
                    "Column projection not supported");
        }
    }

    public static TableTunnel getTableTunnel(EnvironmentSettings settings) {
        TableTunnel tunnel = new TableTunnel(getOdps(settings));
        if (settings.getTunnelEndpoint().isPresent()) {
            tunnel.setEndpoint(settings.getTunnelEndpoint().get());
        }
        return tunnel;
    }

    public static Odps getOdps(EnvironmentSettings settings) {
        return ExecutionEnvironment.create(settings).createOdpsClient();
    }
}
