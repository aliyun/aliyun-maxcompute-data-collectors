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

package org.apache.flink.odps.output.writer;

import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.file.StaticOdpsPartitionWrite;
import org.apache.flink.odps.output.writer.stream.DynamicOdpsPartitionStreamWrite;
import org.apache.flink.odps.output.writer.stream.GroupedOdpsPartitionStreamWrite;
import org.apache.flink.odps.output.writer.stream.StaticOdpsPartitionStreamWrite;
import org.apache.flink.odps.output.writer.upsert.DynamicOdpsPartitionUpsert;
import org.apache.flink.odps.output.writer.upsert.GroupedOdpsPartitionUpsert;
import org.apache.flink.odps.output.writer.upsert.StaticOdpsPartitionUpsert;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.types.Row;

public interface OdpsWriteFactory {

    static <T> OdpsTableWrite<T> createOdpsFileWrite(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isOverwrite,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<T> partitionAssigner) {
        if (isDynamicPartition || supportsGrouping) {
            // TODO: support for cluster mode
            throw new UnsupportedOperationException("Cannot support dynamic partition with file writer");
        } else {
            return new StaticOdpsPartitionWrite<>(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    isOverwrite,
                    writeOptions);
        }
    }

    static <T> OdpsStreamWrite<T> createOdpsStreamWrite(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<T> partitionAssigner) {
        if (isDynamicPartition) {
            if (supportsGrouping) {
                return new GroupedOdpsPartitionStreamWrite<>(
                        odpsConf,
                        projectName,
                        tableName,
                        partition,
                        writeOptions,
                        partitionAssigner
                );
            } else {
                return new DynamicOdpsPartitionStreamWrite<>(
                        odpsConf,
                        projectName,
                        tableName,
                        partition,
                        writeOptions,
                        partitionAssigner
                );
            }
        } else {
            return new StaticOdpsPartitionStreamWrite<>(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    writeOptions);
        }
    }

    static OdpsUpsert<Row> createOdpsUpsert(
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions,
            PartitionAssigner<Row> partitionAssigner) {
        if (isDynamicPartition) {
            if (supportsGrouping) {
                return new GroupedOdpsPartitionUpsert(
                        odpsConf,
                        projectName,
                        tableName,
                        partition,
                        writeOptions,
                        partitionAssigner
                );
            } else {
                return new DynamicOdpsPartitionUpsert(
                        odpsConf,
                        projectName,
                        tableName,
                        partition,
                        writeOptions,
                        partitionAssigner
                );
            }
        } else {
            return new StaticOdpsPartitionUpsert(
                    odpsConf,
                    projectName,
                    tableName,
                    partition,
                    writeOptions);
        }
    }
}
