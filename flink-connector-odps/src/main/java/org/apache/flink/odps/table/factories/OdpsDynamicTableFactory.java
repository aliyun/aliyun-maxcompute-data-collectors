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

package org.apache.flink.odps.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.odps.input.OdpsLookupOptions;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.table.OdpsDynamicTableSink;
import org.apache.flink.odps.table.OdpsDynamicTableSource;
import org.apache.flink.odps.table.OdpsOptions;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.odps.table.OdpsOptions.*;

/** A dynamic table factory implementation for Odps catalog. */
public class OdpsDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private final OdpsConf odpsConf;
    public static final String IDENTIFIER = "odps";

    public OdpsDynamicTableFactory() {
        this.odpsConf = OdpsUtils.getOdpsConf();
    }

    public OdpsDynamicTableFactory(OdpsConf odpsConf) {
        this.odpsConf = odpsConf;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_PATH);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ACCESS_ID);
        set.add(ACCESS_KEY);
        set.add(PROJECT_NAME);
        set.add(END_POINT);
        set.add(RUNNING_MODE);

        set.add(INPUT_SPLIT_SIZE);
        set.add(WRITER_BUFFER_ENABLE);
        set.add(WRITER_BUFFER_SIZE);

        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_MAX_RETRIES);

        set.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        set.add(SINK_BUFFER_FLUSH_INTERVAL);
        set.add(SINK_MAX_RETRIES);
        set.add(SINK_DYNAMIC_PARTITION_LIMIT);
        set.add(SINK_PARALLELISM);
        set.add(PARTITION_DEFAULT_VALUE);
        set.add(PARTITION_ASSIGNER_CLASS);
        return set;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        OdpsConf odpsConf = Preconditions.checkNotNull(getOdpsConf(config),
                "Odps conf cannot be null");;
        return new OdpsDynamicTableSink(
                context.getConfiguration(),
                odpsConf,
                getOdpsStreamWriteOptions(config),
                OdpsTablePath.fromTablePath(odpsConf.getProject(), config.get(TABLE_PATH)),
                schema,
                context.getCatalogTable().getPartitionKeys(),
                config.get(OdpsOptions.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        validateConfigOptions(config);
        OdpsConf odpsConf = Preconditions.checkNotNull(getOdpsConf(config),
                "Odps conf cannot be null");;
        TableSchema schema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new OdpsDynamicTableSource(
                context.getConfiguration(),
                odpsConf,
                getOdpsLookupOptions(config),
                OdpsTablePath.fromTablePath(odpsConf.getProject(), config.get(TABLE_PATH)),
                schema,
                context.getCatalogTable().getPartitionKeys());
    }

    private void validateConfigOptions(ReadableConfig config) {
        checkAllOrNone(
                config,
                new ConfigOption[] {
                        ACCESS_ID,
                        ACCESS_KEY,
                        PROJECT_NAME,
                        END_POINT
                });

        if (config.get(LOOKUP_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
        }

        if (config.get(SINK_MAX_RETRIES) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "The value of '%s' option shouldn't be negative, but is %s.",
                            SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
        }
    }

    private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
        int presentCount = 0;
        for (ConfigOption configOption : configOptions) {
            if (config.getOptional(configOption).isPresent()) {
                presentCount++;
            }
        }
        String[] propertyNames =
                Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
        Preconditions.checkArgument(
                configOptions.length == presentCount || presentCount == 0,
                "Either all or none of the following options should be provided:\n"
                        + String.join("\n", propertyNames));
    }

    private OdpsConf getOdpsConf(ReadableConfig readableConfig) {
        final Optional<String> accessId = readableConfig.getOptional(ACCESS_ID);
        if (accessId.isPresent()) {
            OdpsConf conf =  new OdpsConf(readableConfig.get(ACCESS_ID),
                    readableConfig.get(ACCESS_KEY),
                    readableConfig.get(END_POINT),
                    readableConfig.get(PROJECT_NAME),
                    readableConfig.get(RUNNING_MODE));
            conf.setProperty(INPUT_SPLIT_SIZE.key(),
                    String.valueOf(readableConfig.get(INPUT_SPLIT_SIZE)));
            conf.setProperty(WRITER_BUFFER_ENABLE.key(),
                    String.valueOf(readableConfig.get(WRITER_BUFFER_ENABLE)));
            conf.setProperty(WRITER_BUFFER_SIZE.key(),
                    String.valueOf(readableConfig.get(WRITER_BUFFER_SIZE).getBytes()));
            return conf;
        }
        else {
            // use odps catalog
            return this.odpsConf;
        }
    }

    public static OdpsWriteOptions getOdpsStreamWriteOptions(ReadableConfig tableOptions) {
        OdpsWriteOptions.Builder builder = OdpsWriteOptions.builder();
        builder.setBufferFlushIntervalMillis(
                tableOptions.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
        builder.setBufferFlushMaxRows(tableOptions.get(SINK_BUFFER_FLUSH_MAX_ROWS));
        builder.setBufferFlushMaxSizeInBytes(
                tableOptions.get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes());
        builder.setWriteMaxRetries(tableOptions.get(SINK_MAX_RETRIES));
        builder.setDynamicPartitionLimit(tableOptions.get(SINK_DYNAMIC_PARTITION_LIMIT));
        builder.setDynamicPartitionDefaultValue(tableOptions.get(PARTITION_DEFAULT_VALUE));
        builder.setDynamicPartitionAssignerClass(tableOptions.get(PARTITION_ASSIGNER_CLASS));
        return builder.build();
    }

    public static OdpsLookupOptions getOdpsLookupOptions(ReadableConfig tableOptions) {
        OdpsLookupOptions.Builder builder = OdpsLookupOptions.builder();
        builder.setCacheExpireMs(
                tableOptions.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setMaxRetryTimes(
                tableOptions.get(LOOKUP_MAX_RETRIES));
        return builder.build();
    }
}
