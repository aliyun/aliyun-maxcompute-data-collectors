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

package org.apache.flink.odps.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.odps.sink.common.WriteOperationType;
import org.apache.flink.odps.util.Constants;

import java.time.Duration;

import static org.apache.flink.odps.util.Constants.*;

public class OdpsOptions {

    public static final ConfigOption<String> OPERATION = ConfigOptions
            .key("write.operation")
            .stringType()
            .defaultValue(WriteOperationType.UPSERT.value())
            .withDescription("The write operation, that this write should do");

    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key(Constants.ODPS_PROJECT_NAME)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Odps project to connect.");

    public static final ConfigOption<String> ACCESS_ID =
            ConfigOptions.key(Constants.ODPS_ACCESS_ID)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access id of Odps project");

    public static final ConfigOption<String> ACCESS_KEY =
            ConfigOptions.key(Constants.ODPS_ACCESS_KEY)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access key of Odps project");

    public static final ConfigOption<String> END_POINT =
            ConfigOptions.key(Constants.ODPS_END_POINT)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The end point of Odps project");

    public static final ConfigOption<Boolean> RUNNING_MODE =
            ConfigOptions.key(Constants.ODPS_CLUSTER_MODE)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("The running mode");

    public static final ConfigOption<String> TABLE_PATH =
            ConfigOptions.key(Constants.ODPS_TABLE)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of Odps table to connect.");

    public static final ConfigOption<Integer> INPUT_SPLIT_SIZE =
            ConfigOptions.key(ODPS_INPUT_SPLIT_SIZE)
                    .intType()
                    .defaultValue(DEFAULT_SPLIT_SIZE)
                    .withDescription("The input split size for odps read.");

    public static final ConfigOption<Boolean> WRITER_BUFFER_ENABLE =
            ConfigOptions.key(ODPS_WRITER_BUFFER_ENABLE)
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Use odps buffer writer.");

    public static final ConfigOption<MemorySize> WRITER_BUFFER_SIZE =
            ConfigOptions.key(ODPS_WRITER_BUFFER_SIZE)
                    .memoryType()
                    .defaultValue(MemorySize.parse("64mb"))
                    .withDescription("Odps writer buffer size.");

    public static final ConfigOption<Long> WRITE_BATCH_SIZE = ConfigOptions
            .key("write.batch.size")
            .longType()
            .defaultValue(4096L) // 256MB
            .withDescription("Batch size");

    // lookup
    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(10))
                    .withDescription("The cache TTL (e.g. 10min) for the build table in lookup join. " +
                            "By default the TTL is 10 minutes.");

    public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
            ConfigOptions.key("lookup.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if lookup database failed.");


    public static final ConfigOption<MemorySize> SINK_BUFFER_FLUSH_MAX_SIZE =
            ConfigOptions.key("sink.buffer-flush.max-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("4mb"))
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each "
                                    + "writing request. This can improve performance for writing data to Odps, "
                                    + "but may increase the latency. Can be set to '0' to disable it. ");

    public static final ConfigOption<Integer> SINK_BUFFER_FLUSH_MAX_ROWS =
            ConfigOptions.key("sink.buffer-flush.max-rows")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to Odps, but may increase the latency. "
                                    + "Can be set to '0' to disable it.");

    public static final ConfigOption<Duration> SINK_BUFFER_FLUSH_INTERVAL =
            ConfigOptions.key("sink.buffer-flush.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(300))
                    .withDescription(
                            "Writing option, the interval to flush any buffered rows. "
                                    + "This can improve performance for writing data to Odps, but may increase the latency. ");

    public static final ConfigOption<Integer> SINK_DYNAMIC_PARTITION_LIMIT =
            ConfigOptions.key("sink.dynamic-partition.limit")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The max limit of dynamic partition");

    public static final ConfigOption<Integer> SINK_MAX_RETRIES =
            ConfigOptions.key("sink.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription("The max retry times if writing records to odps failed.");

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<String> PARTITION_DEFAULT_VALUE =
            ConfigOptions.key("sink.partition.default-value")
            .stringType()
            .defaultValue("__DEFAULT_PARTITION__")
            .withDescription("The default partition name in case the dynamic partition" +
                    " column value is null/empty string");

    public static final ConfigOption<String> PARTITION_ASSIGNER_CLASS =
            ConfigOptions.key("sink.partition.assigner.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The assigner class for implement PartitionAssigner interface.");
}
