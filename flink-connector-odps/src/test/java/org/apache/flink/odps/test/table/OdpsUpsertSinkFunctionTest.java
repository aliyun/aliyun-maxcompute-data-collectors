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
import org.apache.flink.odps.output.OdpsUpsertSinkFunction;
import org.apache.flink.odps.output.stream.PartitionAssigner;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.test.util.BookEntry;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.apache.flink.odps.test.util.BookTableUtils.*;
import static org.apache.flink.odps.test.util.BookTableUtils.compareResult;
import static org.apache.flink.odps.test.util.OdpsTestUtils.dropOdpsTable;
import static org.apache.flink.types.RowKind.*;

public class OdpsUpsertSinkFunctionTest {

    private OdpsUpsertSinkFunction odpsUpsertSinkFunction;

    private static OdpsConf odpsConf;

    private static String project;
    private static String table1;

    private static String partitionTable1;
    private static String partitionTable2;
    private static String partitionTable3;

    @BeforeClass
    public static void init() {
        odpsConf = OdpsTestUtils.getOdpsConf();
        project = odpsConf.getProject();
        table1 = "book_entry1";
        partitionTable1 = "book_entry_partition1";
        partitionTable2 = "book_entry_partition2";
        partitionTable3 = "book_entry_partition3";
    }

    public void initTable(String table) {
        OdpsTestUtils.exec(dropOdpsTable(table));
        OdpsTestUtils.exec(getOdpsCreateQueryWithPK(table));
    }

    public void initPartTable(String partTable) {
        OdpsTestUtils.exec(dropOdpsTable(partTable));
        OdpsTestUtils.exec(getOdpsCreatePartitionQueryWithPK(partTable));
    }

    @Test
    public void testUpsertSink() throws Exception {
        initTable(table1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> input = env.fromElements(TEST_DATA);
        input.addSink(buildOdpsUpsertSinkFunction(odpsConf, table1, ""))
                .setParallelism(1);
        env.execute();
    }

    @Test
    public void testUpsertTableWhenCheckpoint() throws Exception {
        initTable(table1);
        List<BookEntry> odpsResult;
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, table1, "");

        odpsUpsertSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(
                true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());

        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(new ArrayList<>(), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(2, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(2, 2));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[3]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(4, 1));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(3, 3));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[4]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(5, 1));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(4, 4));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[5]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(6, 1));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(5, 5));
        odpsResult = QUERY_BY_ODPS_SQL(table1, SELECT_ALL_BOOKS(table1) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1]), odpsResult);

        odpsUpsertSinkFunction.close();
    }

    @Test
    public void testUpsertPartitionWhenCheckpoint() throws Exception {
        initPartTable(partitionTable1);
        String partition = "date=2019-02-07";
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, partitionTable1, partition);

        odpsUpsertSinkFunction.setRuntimeContext(new MockStreamingRuntimeContext(
                true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());

        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 7));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        odpsUpsertSinkFunction.close();
    }

    @Test
    public void testUpsertGroupingPartition() throws Exception {
        initPartTable(partitionTable2);
        String partition = "";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, partitionTable2, partition, true,
                Arrays.asList("date"), staticPartitionSpec, null);

        odpsUpsertSinkFunction.setRuntimeContext(
                new MockStreamingRuntimeContext(true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());
        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 7));
        odpsUpsertSinkFunction.close();

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable2, SELECT_ALL_BOOKS(partitionTable2) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1],
                BOOK_TEST_DATA[2], BOOK_TEST_DATA[4], BOOK_TEST_DATA[5]), odpsResult);
    }

    @Test
    public void testUpsertGroupingPartitionCheckpoint() throws Exception {
        initPartTable(partitionTable2);
        String partition = "";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, partitionTable2, partition, true,
                Arrays.asList("date"), staticPartitionSpec, null);

        odpsUpsertSinkFunction.setRuntimeContext(
                new MockStreamingRuntimeContext(true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());

        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 3));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable2, SELECT_ALL_BOOKS(partitionTable2) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[2]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(3, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(2, 1));
        odpsResult = QUERY_BY_ODPS_SQL(partitionTable2, SELECT_ALL_BOOKS(partitionTable2) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[2], BOOK_TEST_DATA[4]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(5, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(2, 1));
        odpsResult = QUERY_BY_ODPS_SQL(partitionTable2, SELECT_ALL_BOOKS(partitionTable2) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1],
                BOOK_TEST_DATA[2], BOOK_TEST_DATA[4], BOOK_TEST_DATA[5]), odpsResult);

        odpsUpsertSinkFunction.close();
    }

    @Test
    public void testUpsertDynamicPartition() throws Exception {
        initPartTable(partitionTable3);
        String partition = "";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, partitionTable3, partition, false,
                Arrays.asList("date"), staticPartitionSpec, null);

        odpsUpsertSinkFunction.setRuntimeContext(
                new MockStreamingRuntimeContext(true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());
        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 7));
        odpsUpsertSinkFunction.close();

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable3, SELECT_ALL_BOOKS(partitionTable3) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1],
                BOOK_TEST_DATA[2], BOOK_TEST_DATA[4], BOOK_TEST_DATA[5]), odpsResult);
    }

    @Test
    public void testUpsertDynamicPartitionCheckpoint() throws Exception {
        initPartTable(partitionTable3);
        String partition = "";
        Map<String, String> staticPartitionSpec = new LinkedHashMap<>();
        odpsUpsertSinkFunction = buildOdpsUpsertSinkFunction(odpsConf, partitionTable3, partition, false,
                Arrays.asList("date"), staticPartitionSpec, null);

        odpsUpsertSinkFunction.setRuntimeContext(
                new MockStreamingRuntimeContext(true, 1, 0));
        odpsUpsertSinkFunction.open(new Configuration());

        writeData(odpsUpsertSinkFunction, new ReusableIterator(0, 3));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(1, 1));
        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable3, SELECT_ALL_BOOKS(partitionTable3) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[2]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(3, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(2, 1));
        odpsResult = QUERY_BY_ODPS_SQL(partitionTable3, SELECT_ALL_BOOKS(partitionTable3) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1], BOOK_TEST_DATA[2], BOOK_TEST_DATA[4]), odpsResult);

        writeData(odpsUpsertSinkFunction, new ReusableIterator(5, 2));
        odpsUpsertSinkFunction.snapshotState(new StateSnapshotContextSynchronousImpl(2, 1));
        odpsResult = QUERY_BY_ODPS_SQL(partitionTable3, SELECT_ALL_BOOKS(partitionTable3) + ";");
        compareResult(Arrays.asList(BOOK_TEST_DATA[0], BOOK_TEST_DATA[1],
                BOOK_TEST_DATA[2], BOOK_TEST_DATA[4], BOOK_TEST_DATA[5]), odpsResult);

        odpsUpsertSinkFunction.close();
    }

    public static OdpsUpsertSinkFunction buildOdpsUpsertSinkFunction(OdpsConf odpsConf,
                                                                     String tableName,
                                                                     String partition) {
        String projectName = OdpsTestUtils.projectName;
        OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder builder =
                new OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder(odpsConf, projectName, tableName);
        builder.setPartition(partition);
        return builder.build();
    }

    public static OdpsUpsertSinkFunction buildOdpsUpsertSinkFunction(OdpsConf odpsConf,
                                                                     String tableName,
                                                                     String partition,
                                                                     boolean grouping,
                                                                     List<String> partitionColumns,
                                                                     Map<String, String> staticPartitionSpec,
                                                                     PartitionAssigner<Row> partitionAssigner) {
        boolean isPartitioned = partitionColumns != null && !partitionColumns.isEmpty();
        boolean isDynamicPartition = isPartitioned && partitionColumns.size() > staticPartitionSpec.size();
        String projectName = OdpsTestUtils.projectName;
        OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder builder =
                new OdpsUpsertSinkFunction.OdpsUpsertSinkBuilder(odpsConf, projectName, tableName);
        builder.setPartition(partition);
        builder.setDynamicPartition(isDynamicPartition);
        builder.setSupportPartitionGrouping(grouping);
        builder.setPartitionAssigner(partitionAssigner);

        OdpsWriteOptions odpsWriteOptions = OdpsWriteOptions.builder()
                .setDynamicPartitionLimit(200).build();
        builder.setWriteOptions(odpsWriteOptions);
        return builder.build();
    }

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.INT().notNull()),
                    Column.physical("title", DataTypes.STRING().notNull()),
                    Column.physical("author", DataTypes.STRING()),
                    Column.physical("price", DataTypes.DOUBLE()),
                    Column.physical("qty", DataTypes.INT()),
                    Column.physical("date", DataTypes.STRING()));

    public static final BookEntry[] BOOK_TEST_DATA = {
            new BookEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11, "2019-01-09"),
            new BookEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22, "2019-01-08"),
            new BookEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44, "2019-01-08"),
            new BookEntry(1004, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55, "2019-02-09"),
            new BookEntry(1004, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66, "2019-02-09"),
            new BookEntry(1004, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77, "2019-02-07"),
            new BookEntry(1004, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010, "2019-02-08")
    };

    public static final RowData[] TEST_DATA = {
            GenericRowData.ofKind(
                    INSERT,
                    BOOK_TEST_DATA[0].id,
                    StringData.fromString(BOOK_TEST_DATA[0].title),
                    StringData.fromString(BOOK_TEST_DATA[0].author),
                    BOOK_TEST_DATA[0].price,
                    BOOK_TEST_DATA[0].qty,
                    StringData.fromString(BOOK_TEST_DATA[0].date)),
            GenericRowData.ofKind(
                    INSERT,
                    BOOK_TEST_DATA[1].id,
                    StringData.fromString(BOOK_TEST_DATA[1].title),
                    StringData.fromString(BOOK_TEST_DATA[1].author),
                    BOOK_TEST_DATA[1].price,
                    BOOK_TEST_DATA[1].qty,
                    StringData.fromString(BOOK_TEST_DATA[1].date)),
            GenericRowData.ofKind(
                    INSERT,
                    BOOK_TEST_DATA[2].id,
                    StringData.fromString(BOOK_TEST_DATA[2].title),
                    StringData.fromString(BOOK_TEST_DATA[2].author),
                    BOOK_TEST_DATA[2].price,
                    BOOK_TEST_DATA[2].qty,
                    StringData.fromString(BOOK_TEST_DATA[2].date)),
            GenericRowData.ofKind(
                    UPDATE_AFTER,
                    BOOK_TEST_DATA[3].id,
                    StringData.fromString(BOOK_TEST_DATA[3].title),
                    StringData.fromString(BOOK_TEST_DATA[3].author),
                    BOOK_TEST_DATA[3].price,
                    BOOK_TEST_DATA[3].qty,
                    StringData.fromString(BOOK_TEST_DATA[3].date)),
            GenericRowData.ofKind(
                    UPDATE_AFTER,
                    BOOK_TEST_DATA[4].id,
                    StringData.fromString(BOOK_TEST_DATA[4].title),
                    StringData.fromString(BOOK_TEST_DATA[4].author),
                    BOOK_TEST_DATA[4].price,
                    BOOK_TEST_DATA[4].qty,
                    StringData.fromString(BOOK_TEST_DATA[4].date)),
            GenericRowData.ofKind(
                    UPDATE_AFTER,
                    BOOK_TEST_DATA[5].id,
                    StringData.fromString(BOOK_TEST_DATA[5].title),
                    StringData.fromString(BOOK_TEST_DATA[5].author),
                    BOOK_TEST_DATA[5].price,
                    BOOK_TEST_DATA[5].qty,
                    StringData.fromString(BOOK_TEST_DATA[5].date)),
            GenericRowData.ofKind(
                    DELETE,
                    BOOK_TEST_DATA[6].id,
                    StringData.fromString(BOOK_TEST_DATA[6].title),
                    StringData.fromString(BOOK_TEST_DATA[6].author),
                    BOOK_TEST_DATA[6].price,
                    BOOK_TEST_DATA[6].qty,
                    StringData.fromString(BOOK_TEST_DATA[6].date))
    };

    private void writeData(OdpsUpsertSinkFunction sink, Iterator<RowData> iterator)
            throws Exception {
        while (iterator.hasNext()) {
            RowData next = iterator.next();
            sink.invoke(next, SinkContextUtil.forTimestamp(1));
        }
    }

    private class ReusableIterator implements Iterator<RowData> {

        private final RowDataSerializer serializer =
                InternalTypeInfo.of(SCHEMA.toSinkRowDataType().getLogicalType()).toRowSerializer();
        private final RowData reusedRow = new GenericRowData(SCHEMA.getColumnCount());

        private int begin;
        private final int end;

        ReusableIterator(int begin, int size) {
            this.begin = begin;
            this.end = begin + size;
        }

        @Override
        public boolean hasNext() {
            return begin < end;
        }

        @Override
        public RowData next() {
            return serializer.copy(TEST_DATA[begin++], reusedRow);
        }
    }
}
