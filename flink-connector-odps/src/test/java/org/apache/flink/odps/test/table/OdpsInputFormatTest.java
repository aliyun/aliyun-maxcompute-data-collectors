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
import org.apache.flink.odps.input.OdpsInputFormat;
import org.apache.flink.odps.input.OdpsInputSplit;
import org.apache.flink.odps.test.util.BookEntry;
import org.apache.flink.odps.test.util.BookTableUtils;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.odps.test.util.BookEntry.TEST_DATA;

public class OdpsInputFormatTest {

    private OdpsInputFormat<RowData> odpsInputFormat;
    private OdpsInputFormat<Row> odpsInputFormatRow;
    private OdpsInputFormat<BookEntry> odpsInputFormatPojo;
    private OdpsInputFormat<Tuple6<Integer, String, String, Double, Integer, String>> odpsInputFormatTuple;
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
        OdpsTestUtils.exec(BookTableUtils.getOdpsCreateQuery(table));
        OdpsTestUtils.exec(BookTableUtils.getOdpsInsertQuery(table));
        OdpsTestUtils.exec(BookTableUtils.getOdpsCreatePartitionQuery(partitionTable));
        OdpsTestUtils.exec(BookTableUtils.getOdpsInsertPartitionQuery(table, partitionTable));
    }

    @Test
    public void testEmptyResults() throws IOException {
        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, table)
                .build();
        try {
            odpsInputFormat.openInputFormat();
            odpsInputFormat.open(null);
            Assert.assertTrue(odpsInputFormat.reachedEnd());
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectAllColumns() throws IOException {
        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, table)
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    RowData next = odpsInputFormat.nextRecord(null);
                    assertEquals(TEST_DATA[recordCount], next);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            odpsInputFormat.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectAllColumnsRows() throws IOException {
        odpsInputFormatRow = new OdpsInputFormat.OdpsInputFormatBuilder<Row>(odpsConf, project, table)
                .build().asFlinkRows();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormatRow.createInputSplits(1);
            Row row = new Row(6);
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormatRow.open(odpsInputSplit);
                while (!odpsInputFormatRow.reachedEnd()) {
                    Row next = odpsInputFormatRow.nextRecord(row);
                    assertEquals(TEST_DATA[recordCount], next);
                    recordCount++;
                }
                odpsInputFormatRow.close();
            }
            odpsInputFormatRow.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormatRow.close();
            odpsInputFormatRow.closeInputFormat();
        }
    }

    @Test
    public void testSelectAllColumnsPojo() throws IOException {
        odpsInputFormatPojo = new OdpsInputFormat.OdpsInputFormatBuilder<BookEntry>(odpsConf, project, table)
                .build().asPojos(BookEntry.class);
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormatPojo.createInputSplits(1);
            BookEntry bookEntry = new BookEntry();
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormatPojo.open(odpsInputSplit);
                while (!odpsInputFormatPojo.reachedEnd()) {
                    BookEntry next = odpsInputFormatPojo.nextRecord(bookEntry);
                    Assert.assertEquals(TEST_DATA[recordCount], next);
                    recordCount++;
                }
                odpsInputFormatPojo.close();
            }
            odpsInputFormatPojo.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormatPojo.close();
            odpsInputFormatPojo.closeInputFormat();
        }
    }

    @Test
    public void testSelectAllColumnsTuples() throws IOException {
        odpsInputFormatTuple = new OdpsInputFormat
                .OdpsInputFormatBuilder<Tuple6<Integer, String, String, Double, Integer, String>>(odpsConf, project, table)
                .build().asFlinkTuples();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormatTuple.createInputSplits(1);
            Tuple6 tuple6 = new Tuple6<>();
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormatTuple.open(odpsInputSplit);
                while (!odpsInputFormatTuple.reachedEnd()) {
                    Tuple6 next = odpsInputFormatTuple.nextRecord(tuple6);
                    BookEntry bookEntry = new BookEntry();
                    bookEntry.setId((Integer) next.getField(0));
                    bookEntry.setTitle((String) next.getField(1));
                    bookEntry.setAuthor((String) next.getField(2));
                    bookEntry.setPrice((Double) next.getField(3));
                    bookEntry.setQty((Integer) next.getField(4));
                    bookEntry.setDate((String) next.getField(5));
                    Assert.assertEquals(TEST_DATA[recordCount], bookEntry);
                    recordCount++;
                }
                odpsInputFormatTuple.close();
            }
            odpsInputFormatTuple.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormatTuple.close();
            odpsInputFormatTuple.closeInputFormat();
        }
    }


    @Test
    public void testSelectZeroColumns() throws IOException {
        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, table)
                .setColumns(new String[0])
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    RowData next = odpsInputFormat.nextRecord(null);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            odpsInputFormat.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectRandomColumns() throws IOException {
        String[] columns = new String[3];
        columns[0] = "id";
        columns[1] = "date";
        columns[2] = "qty";

        List<GenericRowData> expectRowList = new ArrayList<>();
        for (BookEntry testDatum : TEST_DATA) {
            GenericRowData row = new GenericRowData(3);
            row.setField(0, testDatum.id);
            row.setField(1, StringData.fromString(testDatum.date));
            row.setField(2, testDatum.qty);
            expectRowList.add(row);
        }

        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, table)
                .setColumns(columns)
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);
            int recordCount = 0;
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    GenericRowData next = (GenericRowData)odpsInputFormat.nextRecord(null);
                    assertEquals(expectRowList.get(recordCount), next, 3);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            odpsInputFormat.closeInputFormat();
            Assert.assertEquals(TEST_DATA.length, recordCount);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectRandomColumnsWithPartitionTable() throws IOException {
        String[] columns = new String[4];
        columns[0] = "id";
        columns[1] = "date";
        columns[2] = "qty";
        columns[3] = "author";

        List<BookEntry> expectBookList = new ArrayList<>();
        for (int i = 0; i < TEST_DATA.length; i++) {
            BookEntry bookEntry  = new BookEntry();
            bookEntry.setId(TEST_DATA[i].id);
            bookEntry.setDate(TEST_DATA[i].date);
            bookEntry.setQty(TEST_DATA[i].qty);
            bookEntry.setAuthor(TEST_DATA[i].author);
            expectBookList.add(bookEntry);
        }

        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, partitionTable)
                .setColumns(columns)
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);
            int recordCount = 0;
            List<GenericRowData> actualList = new ArrayList<>();
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    GenericRowData next = (GenericRowData) odpsInputFormat.nextRecord(null);
                    actualList.add(next);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            Assert.assertEquals(TEST_DATA.length, recordCount);
            BookTableUtils.compareRowDataResult(actualList, expectBookList, columns);
            odpsInputFormat.closeInputFormat();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectRandomPartitionWithPartitionTable() throws IOException {
        String[] compareColumns = new String[6];
        compareColumns[0] = "id";
        compareColumns[1] = "title";
        compareColumns[2] = "price";
        compareColumns[3] = "qty";
        compareColumns[4] = "author";
        compareColumns[5] = "date";

        String[] partitions = new String[2];
        partitions[0] = "author=Kevin Jones, date=2019-02-07";
        partitions[1] = "author=Kumar,date=2019-01-08";

        List<BookEntry> expectBookList = new ArrayList<>();
        expectBookList.add(TEST_DATA[3]);
        expectBookList.add(TEST_DATA[6]);
        expectBookList.add(TEST_DATA[7]);

        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, partitionTable)
                .setPartitions(partitions)
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);
            int recordCount = 0;
            List<GenericRowData> actualList = new ArrayList<>();
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    GenericRowData next = (GenericRowData)odpsInputFormat.nextRecord(null);
                    actualList.add(next);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            BookTableUtils.compareRowDataResult(actualList, expectBookList, compareColumns);
            odpsInputFormat.closeInputFormat();
            Assert.assertEquals(3, recordCount);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    @Test
    public void testSelectZeroColumnWithPartitionTable() throws IOException {
        String[] partitions = new String[2];
        partitions[0] = "author=Kevin Jones, date=2019-02-07";
        partitions[1] = "author=Kumar,date=2019-01-08";

        List<BookEntry> expectBookList = new ArrayList<>();
        BookEntry bookEntry  = new BookEntry();
        BookEntry bookEntry1  = new BookEntry();
        BookEntry bookEntry2  = new BookEntry();
        bookEntry.setAuthor(TEST_DATA[3].author);
        bookEntry.setDate(TEST_DATA[3].date);
        bookEntry1.setAuthor(TEST_DATA[6].author);
        bookEntry1.setDate(TEST_DATA[6].date);
        bookEntry2.setAuthor(TEST_DATA[7].author);
        bookEntry2.setDate(TEST_DATA[7].date);

        String[] compareColumns = new String[2];
        compareColumns[0] = "author";
        compareColumns[1] = "date";

        expectBookList.add(bookEntry);
        expectBookList.add(bookEntry1);
        expectBookList.add(bookEntry2);

        odpsInputFormat = new OdpsInputFormat.OdpsInputFormatBuilder<RowData>(odpsConf, project, partitionTable)
                .setColumns(compareColumns)
                .setPartitions(partitions)
                .build();
        try {
            OdpsInputSplit[] odpsInputSplits = odpsInputFormat.createInputSplits(1);

            int recordCount = 0;
            List<GenericRowData> actualList = new ArrayList<>();
            for (OdpsInputSplit odpsInputSplit : odpsInputSplits) {
                odpsInputFormat.open(odpsInputSplit);
                while (!odpsInputFormat.reachedEnd()) {
                    GenericRowData next = (GenericRowData)odpsInputFormat.nextRecord(null);
                    actualList.add(next);
                    recordCount++;
                }
                odpsInputFormat.close();
            }
            odpsInputFormat.closeInputFormat();
            BookTableUtils.compareRowDataResult(actualList, expectBookList, compareColumns);
            Assert.assertEquals(3, recordCount);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            odpsInputFormat.close();
            odpsInputFormat.closeInputFormat();
        }
    }

    private static void assertEquals(BookEntry expected, RowData actual) {
        Assert.assertEquals(expected.id, Integer.valueOf(actual.getInt(0)));
        Assert.assertEquals(expected.title, actual.getString(1).toString());
        Assert.assertEquals(expected.author, actual.getString(2).toString());
        Assert.assertEquals(expected.price, Double.valueOf(actual.getDouble(3)));
        Assert.assertEquals(expected.qty, Integer.valueOf(actual.getInt(4)));
        Assert.assertEquals(expected.date, actual.getString(5).toString());
    }

    private static void assertEquals(BookEntry expected, Row actual) {
        Assert.assertEquals(expected.id, actual.getField(0));
        Assert.assertEquals(expected.title, actual.getField(1));
        Assert.assertEquals(expected.author, actual.getField(2));
        Assert.assertEquals(expected.price, actual.getField(3));
        Assert.assertEquals(expected.qty, actual.getField(4));
        Assert.assertEquals(expected.date, actual.getField(5));
    }

    private static void assertEquals(GenericRowData expected, GenericRowData actual, int arity) {
        for(int i = 0 ;i < arity ; i++) {
            Assert.assertEquals(expected.getField(i), actual.getField(i));
        }
    }
}
