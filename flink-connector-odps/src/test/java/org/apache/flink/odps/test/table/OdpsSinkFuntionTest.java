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

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.OdpsSinkFunction;
import org.apache.flink.odps.output.stream.DateTimePartitionAssigner;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.test.util.BookEntry;
import org.apache.flink.odps.test.util.MockSinkContext;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.flink.odps.test.util.BookEntry.*;
import static org.apache.flink.odps.test.util.BookTableUtils.*;
import static org.apache.flink.odps.test.util.OdpsTestUtils.dropOdpsTable;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;

public class OdpsSinkFuntionTest {

    private OdpsSinkFunction<Row> odpsSinkFunction;
    private OdpsSinkFunction<BookEntry> odpsSinkFunctionPojo;
    private OdpsSinkFunction<Tuple6<Integer, String, String, Double, Integer, String>> odpsSinkFunctionTuple;

    private static OdpsConf odpsConf;
    private static String project;
    private static String table1;
    private static String table2;
    private static String table3;
    private static String table4;

    private static String partitionTable1;
    private static String partitionTable2;
    private static String partitionTable3;
    private static String partitionTable4;
    private static String partitionTable5;
    private long time = 1000L;

    @BeforeClass
    public static void init() {
        odpsConf = OdpsTestUtils.getOdpsConf();
        project = odpsConf.getProject();
        table1 = "book_entry1";
        table2 = "book_entry2";
        table3 = "book_entry3";
        table4 = "book_entry4";

        partitionTable1 = "book_entry_partition1";
        partitionTable2 = "book_entry_partition2";
        partitionTable3 = "book_entry_partition3";
        partitionTable4 = "book_entry_partition4";
        partitionTable5 = "book_entry_partition5";
    }

    public void initTable(String table) {
        OdpsTestUtils.exec(dropOdpsTable(table));
        OdpsTestUtils.exec(getOdpsCreateQuery(table));
    }

    public void initPartTable(String partTable) {
        OdpsTestUtils.exec(dropOdpsTable(partTable));
        OdpsTestUtils.exec(getOdpsCreatePartitionQuery(partTable));
    }

    public static <T> OdpsSinkFunction<T> buildOdpsSinkFunction(OdpsConf odpsConf,
                                                                String tableName,
                                                                String partition,
                                                                boolean grouping,
                                                                List<String> partitionColumns,
                                                                Map<String, String> staticPartitionSpec,
                                                                PartitionAssigner<T> partitionAssigner) {
        boolean isPartitioned = partitionColumns != null && !partitionColumns.isEmpty();
        boolean isDynamicPartition = isPartitioned && partitionColumns.size() > staticPartitionSpec.size();
        String projectName = OdpsTestUtils.projectName;
        OdpsSinkFunction.OdpsSinkBuilder<T> builder =
                new OdpsSinkFunction.OdpsSinkBuilder<>(odpsConf, projectName, tableName);
        builder.setPartition(partition);
        builder.setDynamicPartition(isDynamicPartition);
        builder.setSupportPartitionGrouping(grouping);
        builder.setPartitionAssigner(partitionAssigner);
        return builder.build();
    }

    @Test
    public void testFlushBufferWhenCheckpoint() throws Exception {
        initTable(table1);
        List<BookEntry> odpsResult;
        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, table1, "", false, new ArrayList<>(), new HashMap<>(), null);

        odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(
                true, 1, 0));
        odpsSinkFunction.open(new Configuration());
        odpsSinkFunction.invoke(toRow(TEST_DATA[0]), new MockSinkContext(time, time, time));
        odpsSinkFunction.invoke(toRow(TEST_DATA[1]), new MockSinkContext(time, time, time));

        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(new ArrayList<>(), odpsResult);

        odpsSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(TEST_DATA[0], TEST_DATA[1]), odpsResult);
        odpsSinkFunction.close();
    }

    @Test
    public void testOdpsSinkFunction() throws Exception {
        initTable(table2);
        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, table2, "", false, new ArrayList<>(), new HashMap<>(), null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toRow(TEST_DATA[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table2, SELECT_ALL_BOOKS(table2) + ";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionPojo() throws Exception {
        initTable(table3);
        odpsSinkFunctionPojo = buildOdpsSinkFunction(odpsConf, table3, "", false, new ArrayList<>(), new HashMap<>(), null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunctionPojo.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunctionPojo.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunctionPojo.invoke(TEST_DATA[i * 2 + j], new MockSinkContext(time, time, time));
            }
            odpsSinkFunctionPojo.close();
        }
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table3, SELECT_ALL_BOOKS(table3) + ";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionTuple() throws Exception {
        initTable(table4);
        odpsSinkFunctionTuple = buildOdpsSinkFunction(odpsConf, table4, "", false, new ArrayList<>(), new HashMap<>(), null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunctionTuple.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunctionTuple.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunctionTuple.invoke(toTuple(TEST_DATA[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunctionTuple.close();
        }
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table4, SELECT_ALL_BOOKS(table4) + ";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionPartition() throws Exception {
        initPartTable(partitionTable1);
        String partition = "author=Kevin Jones, date=2019-02-07";
        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable1, partition, false, new ArrayList<>(), new HashMap<>(), null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toPartitionRow(TEST_DATA[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable1, SELECT_ALL_BOOKS(partitionTable1) + ";");
        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA).map(bookEntry -> {
            bookEntry.setAuthor("Kevin Jones");
            bookEntry.setDate("2019-02-07");
            return bookEntry;
        }).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionGrouping() throws Exception {
        initPartTable(partitionTable2);
        String partition = "author=Kevin Jones";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        staticPartitionSpec.put("author", "Kevin Jones");
        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable2, partition, true, Arrays.asList("author", "date"), staticPartitionSpec, null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toPartitionRow(TEST_DATA_GROUPED_BY_DATE[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable2, SELECT_ALL_BOOKS(partitionTable2) + ";");
        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA_GROUPED_BY_DATE).map(bookEntry -> {
            bookEntry.setAuthor("Kevin Jones");
            return bookEntry;
        }).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionGrouping2() throws Exception {
        initPartTable(partitionTable3);
        String partition = "";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable3, partition, true, Arrays.asList("author", "date"), staticPartitionSpec, null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toPartitionRow(TEST_DATA_GROUPED_BY_DATE[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable3, SELECT_ALL_BOOKS(partitionTable3) + ";");
        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA_GROUPED_BY_DATE).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionUsePartitionAssigner() throws Exception {
        initPartTable(partitionTable4);
        String partition = "author=Kevin Jones";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        staticPartitionSpec.put("author", "Kevin Jones");

        time = System.currentTimeMillis();
        DateTimePartitionAssigner<Row> dateTimePartitionAssigner = new DateTimePartitionAssigner<>("date",
                DateTimePartitionAssigner.DEFAULT_FORMAT_STRING);

        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable4, partition, true, Arrays.asList("author", "date"), staticPartitionSpec, dateTimePartitionAssigner);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toPartitionRow(TEST_DATA_GROUPED_BY_DATE[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable4, SELECT_ALL_BOOKS(partitionTable4) + ";");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd--HH").withZone(ZoneId.systemDefault());

        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA_GROUPED_BY_DATE).map(bookEntry -> {
            bookEntry.setAuthor("Kevin Jones");
            bookEntry.setDate(dateTimeFormatter.format(Instant.ofEpochMilli(time)));
            return bookEntry;
        }).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }


    @Test
    public void testOdpsSinkFunctionDynamic() throws Exception {
        initPartTable(partitionTable5);
        String partition = "author=Kevin Jones";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        staticPartitionSpec.put("author", "Kevin Jones");

        odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable5, partition, false, Arrays.asList("author", "date"), staticPartitionSpec, null);
        for (int i = 0; i < 5; i++) {
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, i));
            odpsSinkFunction.open(new Configuration());
            for (int j = 0; j < 2; j++) {
                odpsSinkFunction.invoke(toPartitionRow(TEST_DATA_GROUPED_BY_DATE[i * 2 + j]), new MockSinkContext(time, time, time));
            }
            odpsSinkFunction.close();
        }

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable5, SELECT_ALL_BOOKS(partitionTable5) + ";");
        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA_GROUPED_BY_DATE).map(bookEntry -> {
            bookEntry.setAuthor("Kevin Jones");
            return bookEntry;
        }).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }

    @Test
    public void testOdpsSinkFunctionClusterMode() {
        initPartTable(table1);
        String expectedMsg = "Odps sink function cannot support overwrite in cluster mode";
        try {
            odpsConf.setClusterMode(true);
            odpsSinkFunction = buildOdpsSinkFunction(odpsConf, table1, "", false, new ArrayList<>(), new HashMap<>(), null);
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, 1));
            odpsSinkFunction.open(new Configuration());
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testOdpsSinkFunctionGroupingInvalidPartition() throws Exception {
        initPartTable(partitionTable1);
        String expectedMsg = "Dynamic partition cannot appear before static partition";
        try {
            String partition = "date=11";
            Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
            staticPartitionSpec.put("date", "11");
            odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable1, partition, true, Arrays.asList("author", "date"), staticPartitionSpec, null);
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, 1));
            odpsSinkFunction.open(new Configuration());
        } catch (Exception e) {
            assertTrue(findThrowable(e, IllegalArgumentException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testOdpsSinkFunctionDynamicInvalidPartition() throws Exception {
        initPartTable(partitionTable1);
        String expectedMsg = "Dynamic partition cannot appear before static partition";
        try {
            String partition = "date=11";
            Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
            staticPartitionSpec.put("date", "11");
            odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable1, partition, false, Arrays.asList("author", "date"), staticPartitionSpec, null);
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, 1));
            odpsSinkFunction.open(new Configuration());
        } catch (Exception e) {
            assertTrue(findThrowable(e, IllegalArgumentException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }


    @Test
    public void testOdpsSinkFunctionInvalidPartition() {
        initPartTable(partitionTable1);
        String expectedMsg = "java.io.IOException: check partition failed.";
        try {
            odpsSinkFunction = buildOdpsSinkFunction(odpsConf, partitionTable1, "", false, new ArrayList<>(), new HashMap<>(), null);
            odpsSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(false, 5, 1));
            odpsSinkFunction.open(new Configuration());
        } catch (Exception e) {
            assertTrue(findThrowable(e, FlinkOdpsException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }
}
