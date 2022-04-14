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
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.OdpsOutputFormat;
import org.apache.flink.odps.test.util.BookEntry;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.apache.flink.odps.test.util.BookEntry.TEST_DATA;
import static org.apache.flink.odps.test.util.BookTableUtils.*;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;

public class OdpsOutputFormatTest {

    private OdpsOutputFormat<Row> odpsOutputFormat;
    private OdpsOutputFormat<BookEntry> odpsOutputFormatPojo;
    private OdpsOutputFormat<Tuple6<Integer, String, String, Double, Integer, String>> odpsOutputFormatTuple;

    private static OdpsConf odpsConf;
    private static String project;
    private static String table;
    private static String partitionTable;

    @BeforeClass
    public static void init() {
        odpsConf = OdpsTestUtils.getOdpsConf();
        project = odpsConf.getProject();
        table = "book_entry";
        partitionTable = "book_entry_partition";
        initTable();
    }

    public static void initTable() {
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(table));
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(partitionTable));
        OdpsTestUtils.exec(getOdpsCreateQuery(table));
        OdpsTestUtils.exec(getOdpsCreatePartitionQuery(partitionTable));
    }

    private <T> OdpsOutputFormat<T> buildOdpsOutputFormat(OdpsConf odpsConf,
                                                          String tableName,
                                                          String partition,
                                                          boolean grouping,
                                                          List<String> partitionColumns,
                                                          Map<String, String> staticPartitionSpec,
                                                          boolean overwrite) {
        boolean isPartitioned =  partitionColumns != null && !partitionColumns.isEmpty();
        boolean isDynamicPartition = isPartitioned && partitionColumns.size() > staticPartitionSpec.size();
        String projectName = OdpsTestUtils.projectName;
        OdpsOutputFormat.OutputFormatBuilder<T> builder =
                new OdpsOutputFormat.OutputFormatBuilder<>(odpsConf, projectName, tableName);
        builder.setOverwrite(overwrite);
        builder.setPartition(partition);
        builder.setDynamicPartition(isDynamicPartition);
        builder.setSupportPartitionGrouping(grouping);
        return builder.build();
    }

    @Test
    public void testOdpsOutputFormat() throws Exception {
        odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",false,new ArrayList<>(), new HashMap<>(), true);
        odpsOutputFormat.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormat.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormat.writeRecord(toRow(TEST_DATA[i*2+j]));
            }
            odpsOutputFormat.close();
        }
        odpsOutputFormat.finalizeGlobal(5);

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table, SELECT_ALL_BOOKS(table)+";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsOutputFormatAppend() throws Exception {
        odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",false,new ArrayList<>(), new HashMap<>(), true);
        odpsOutputFormat.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormat.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormat.writeRecord(toRow(TEST_DATA[i*2+j]));
            }
            odpsOutputFormat.close();
        }
        odpsOutputFormat.finalizeGlobal(5);

        odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",false,new ArrayList<>(), new HashMap<>(), false);
        odpsOutputFormat.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormat.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormat.writeRecord(toRow(TEST_DATA[i*2+j]));
            }
            odpsOutputFormat.close();
        }
        odpsOutputFormat.finalizeGlobal(5);

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table, SELECT_ALL_BOOKS(table)+";");
        List<BookEntry> expected = new ArrayList<>();
        expected.addAll(Arrays.asList(TEST_DATA));
        expected.addAll(Arrays.asList(TEST_DATA));
        compareResult(expected, odpsResult);
    }

    @Test
    public void testOdpsOutputFormatPojo() throws Exception {
        odpsOutputFormatPojo = buildOdpsOutputFormat(odpsConf, table, "",false,new ArrayList<>(), new HashMap<>(), true);
        odpsOutputFormatPojo.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormatPojo.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormatPojo.writeRecord(TEST_DATA[i*2+j]);
            }
            odpsOutputFormatPojo.close();
        }
        odpsOutputFormatPojo.finalizeGlobal(5);

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table, SELECT_ALL_BOOKS(table)+";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsOutputFormatTuple() throws Exception {
        odpsOutputFormatTuple = buildOdpsOutputFormat(odpsConf, table, "",false,new ArrayList<>(), new HashMap<>(), true);
        odpsOutputFormatTuple.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormatTuple.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormatTuple.writeRecord(toTuple(TEST_DATA[i*2+j]));
            }
            odpsOutputFormatTuple.close();
        }
        odpsOutputFormatTuple.finalizeGlobal(5);

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(table, SELECT_ALL_BOOKS(table)+";");
        compareResult(Arrays.asList(TEST_DATA), odpsResult);
    }

    @Test
    public void testOdpsOutputFormatPartition() throws Exception {
        String partition = "author=Kevin Jones, date=2019-02-07";
        odpsOutputFormat = buildOdpsOutputFormat(odpsConf, partitionTable, partition,false, new ArrayList<>(), new HashMap<>(), false);
        odpsOutputFormat.initializeGlobal(5);
        for (int i = 0; i < 5 ; i++) {
            odpsOutputFormat.open(i, 5);
            for (int j = 0; j < 2; j++) {
                odpsOutputFormat.writeRecord(toPartitionRow(TEST_DATA[i*2+j]));
            }
            odpsOutputFormat.close();
        }
        odpsOutputFormat.finalizeGlobal(5);

        List<BookEntry> odpsResult = QUERY_BY_ODPS_SQL(partitionTable, SELECT_ALL_BOOKS(partitionTable)+";");
        BookEntry[] modifyPartitionCol = Arrays.stream(TEST_DATA).map(bookEntry -> {
            bookEntry.setAuthor("Kevin Jones");
            bookEntry.setDate("2019-02-07");
            return bookEntry;
        }).toArray(BookEntry[]::new);
        compareResult(Arrays.asList(modifyPartitionCol), odpsResult);
    }

    @Test
    public void testOdpsOutputFormatClusterMode() {
        String expectedMsg = "cupid-native";
        try {
            odpsConf.setClusterMode(true);
            odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",false, new ArrayList<>(), new HashMap<>(), false);
            odpsOutputFormat.initializeGlobal(5);
        } catch (Exception e) {
            assertTrue(findThrowable(e, ClassNotFoundException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testOdpsOutputFormatGrouping() {
        String expectedMsg = "Cannot support dynamic partition in local mode by OdpsOutputFormat";
        try {
            odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",true, new ArrayList<>(), new HashMap<>(), false);
        } catch (Exception e) {
            assertTrue(findThrowable(e, UnsupportedOperationException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testOdpsOutputFormatDynamic() {
        String expectedMsg = "Cannot support dynamic partition in local mode by OdpsOutputFormat";
        try {
            odpsOutputFormat = buildOdpsOutputFormat(odpsConf, table, "",false, Arrays.asList("author","date"), new HashMap<>(), false);
        } catch (Exception e) {
            assertTrue(findThrowable(e, UnsupportedOperationException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    @Test
    public void testOdpsOutputFormatInvalidPartition() {
        String expectedMsg = "java.io.IOException: check partition failed.";
        try {
            odpsOutputFormat = buildOdpsOutputFormat(odpsConf, partitionTable, "",false, new ArrayList<>(), new HashMap<>(), false);
        } catch (Exception e) {
            assertTrue(findThrowable(e, FlinkOdpsException.class).isPresent());
            assertTrue(findThrowableWithMessage(e, expectedMsg).isPresent());
        }
    }

    static Row toRow(BookEntry entry) {
        Row row = new Row(6);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.author);
        row.setField(3, entry.price);
        row.setField(4, entry.qty);
        row.setField(5, entry.date);
        return row;
    }

    static Row toPartitionRow(BookEntry entry) {
        Row row = new Row(6);
        row.setField(0, entry.id);
        row.setField(1, entry.title);
        row.setField(2, entry.price);
        row.setField(3, entry.qty);
        row.setField(4, entry.author);
        row.setField(5, entry.date);
        return row;
    }

    static Tuple6 toTuple(BookEntry entry) {
        Tuple6<Integer, String, String, Double, Integer, String> tuple6 = new Tuple6<>();
        tuple6.setField(entry.id, 0);
        tuple6.setField(entry.title, 1);
        tuple6.setField(entry.author, 2);
        tuple6.setField(entry.price, 3);
        tuple6.setField(entry.qty,4);
        tuple6.setField(entry.date,5);
        return tuple6;
    }
}
