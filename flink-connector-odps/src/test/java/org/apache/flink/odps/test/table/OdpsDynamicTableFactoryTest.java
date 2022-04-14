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

package org.apache.flink.odps.test.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.input.OdpsLookupOptions;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.table.OdpsDynamicTableSink;
import org.apache.flink.odps.table.OdpsDynamicTableSource;
import org.apache.flink.odps.table.OdpsTablePath;
import org.apache.flink.odps.table.factories.OdpsDynamicTableFactory;
import org.apache.flink.odps.util.Constants;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Test;

import java.util.*;

import static org.apache.flink.odps.table.OdpsOptions.*;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.*;

public class OdpsDynamicTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    null);

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "odps");
        options.put(Constants.ODPS_TABLE, "project.tableName");
        options.put(Constants.ODPS_ACCESS_ID, "accessId");
        options.put(Constants.ODPS_ACCESS_KEY, "accessKey");
        options.put(Constants.ODPS_END_POINT, "endpoint");
        options.put(Constants.ODPS_PROJECT_NAME, "project");
        return options;
    }

    private OdpsConf getOdpsConf() {
        OdpsConf odpsConf = new OdpsConf("accessId",
                "accessKey",
                "endpoint",
                "project");
        odpsConf.setProperty(Constants.ODPS_INPUT_SPLIT_SIZE,
                String.valueOf(INPUT_SPLIT_SIZE.defaultValue()));
        odpsConf.setProperty(Constants.ODPS_WRITER_BUFFER_ENABLE,
                String.valueOf(WRITER_BUFFER_ENABLE.defaultValue()));
        odpsConf.setProperty(Constants.ODPS_WRITER_BUFFER_SIZE,
                String.valueOf(WRITER_BUFFER_SIZE.defaultValue().getBytes()));
        return odpsConf;
    }

    @Test
    public void testOdpsProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        OdpsDynamicTableSource expectedSource =
                new OdpsDynamicTableSource(
                        new Configuration(),
                        getOdpsConf(),
                        OdpsDynamicTableFactory.getOdpsLookupOptions(new Configuration()),
                        OdpsTablePath.fromTablePath("project.tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>());
        assertEquals(actualSource, expectedSource);

        // validation for sink
        DynamicTableSink actualSink = createTableSink(SCHEMA, properties);
        OdpsDynamicTableSink expectedSink =
                new OdpsDynamicTableSink(
                        new Configuration(),
                        getOdpsConf(),
                        OdpsDynamicTableFactory.getOdpsStreamWriteOptions(new Configuration()),
                        OdpsTablePath.fromTablePath("project.tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>(),
                        null);
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testOdpsLookupProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("lookup.cache.ttl", "10s");
        properties.put("lookup.max-retries", "10");

        DynamicTableSource actual = createTableSource(SCHEMA, properties);

        OdpsLookupOptions lookupOptions =
                OdpsLookupOptions.builder()
                        .setCacheExpireMs(10_000)
                        .setMaxRetryTimes(10)
                        .build();

        OdpsDynamicTableSource expected =
                new OdpsDynamicTableSource(
                        new Configuration(),
                        getOdpsConf(),
                        lookupOptions,
                        OdpsTablePath.fromTablePath("project.tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>());
        assertEquals(expected, actual);
    }

    @Test
    public void testOdpsSinkProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("sink.buffer-flush.max-rows", "1000");
        properties.put("sink.buffer-flush.interval", "2min");
        properties.put("sink.max-retries", "5");
        properties.put("sink.parallelism", "2");
        DynamicTableSink actualSink = createTableSink(SCHEMA, properties);

        OdpsWriteOptions options = OdpsWriteOptions.builder()
                .setBufferFlushIntervalMillis(120 * 1000)
                .setBufferFlushMaxRows(1000)
                .setWriteMaxRetries(5)
                .setBufferFlushMaxSizeInBytes(new Configuration().get(SINK_BUFFER_FLUSH_MAX_SIZE).getBytes())
                .setDynamicPartitionLimit(new Configuration().get(SINK_DYNAMIC_PARTITION_LIMIT))
                .setDynamicPartitionAssignerClass(new Configuration().get(PARTITION_ASSIGNER_CLASS))
                .setDynamicPartitionDefaultValue(new Configuration().get(PARTITION_DEFAULT_VALUE)).build();

        OdpsDynamicTableSink expectedSink =
                new OdpsDynamicTableSink(
                        new Configuration(),
                        getOdpsConf(),
                        options,
                        OdpsTablePath.fromTablePath("project.tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>(),
                        2);
        assertEquals(expectedSink, actualSink);
    }

    @Test
    public void testOdpsTablePath() {
        // test default project
        Map<String, String> options = new HashMap<>();
        options.put("connector", "odps");
        options.put(Constants.ODPS_TABLE, "tableName");
        options.put(Constants.ODPS_ACCESS_ID, "accessId");
        options.put(Constants.ODPS_ACCESS_KEY, "accessKey");
        options.put(Constants.ODPS_END_POINT, "endpoint");
        options.put(Constants.ODPS_PROJECT_NAME, "project");
        DynamicTableSource actual = createTableSource(SCHEMA, options);

        OdpsDynamicTableSource expected =
                new OdpsDynamicTableSource(
                        new Configuration(),
                        getOdpsConf(),
                        OdpsDynamicTableFactory.getOdpsLookupOptions(new Configuration()),
                        OdpsTablePath.fromTablePath("project", "tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>());
        assertEquals(expected, actual);

        // test other project
        Map<String, String> options2 = new HashMap<>();
        options2.put("connector", "odps");
        options2.put(Constants.ODPS_TABLE, "project2.tableName");
        options2.put(Constants.ODPS_ACCESS_ID, "accessId");
        options2.put(Constants.ODPS_ACCESS_KEY, "accessKey");
        options2.put(Constants.ODPS_END_POINT, "endpoint");
        options2.put(Constants.ODPS_PROJECT_NAME, "project");
        DynamicTableSource actual2 = createTableSource(SCHEMA, options2);
        OdpsDynamicTableSource expected2 =
                new OdpsDynamicTableSource(
                        new Configuration(),
                        getOdpsConf(),
                        OdpsDynamicTableFactory.getOdpsLookupOptions(new Configuration()),
                        OdpsTablePath.fromTablePath("project2.tableName"),
                        TableSchema.fromResolvedSchema(SCHEMA),
                        new ArrayList<>());
        assertEquals(expected2, actual2);
    }

    @Test
    public void testOdpsValidation() {
        // no table path
        try {
            Map<String, String> options = new HashMap<>();
            options.put("connector", "odps");
            createTableSource(SCHEMA, options);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "One or more required options are missing.\n" +
                                    "\n" +
                                    "Missing required options are:\n" +
                                    "\n" +
                                    "table-name")
                            .isPresent());
        }

        // odps conf not complete
        try {
            Map<String, String> options = new HashMap<>();
            options.put("connector", "odps");
            options.put(Constants.ODPS_TABLE, "tableName");
            options.put(Constants.ODPS_ACCESS_ID, "accessId");
            createTableSource(SCHEMA, options);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "Either all or none of the following options should be provided:\n"
                                    + "odps.access.id\nodps.access.key")
                            .isPresent());
        }

        // lookup retries shouldn't be negative
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("lookup.max-retries", "-1");
            createTableSource(SCHEMA, properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "The value of 'lookup.max-retries' option shouldn't be negative, but is -1.")
                            .isPresent());
        }

        // sink retries shouldn't be negative
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("sink.max-retries", "-1");
            createTableSource(SCHEMA, properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                            t,
                            "The value of 'sink.max-retries' option shouldn't be negative, but is -1.")
                            .isPresent());
        }
    }
}
