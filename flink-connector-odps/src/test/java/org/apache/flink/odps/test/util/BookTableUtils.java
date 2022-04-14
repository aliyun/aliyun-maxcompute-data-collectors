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

package org.apache.flink.odps.test.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.odps.test.util.BookEntry.TEST_DATA;
import static org.junit.Assert.assertEquals;

public class BookTableUtils {
    public static final String bookTableName = "flink_connector_book";
    public static final String bookPartTableName = "flink_connector_book_part";

    public static List<Row> generateBookRows(int numRecords, TableSchema schema) {
        int arity = schema.getFieldCount();
        List<Row> res = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Row row = new Row(arity);
            row.setField(0, TEST_DATA[i].id);
            row.setField(1, TEST_DATA[i].title);
            row.setField(2, TEST_DATA[i].author);
            row.setField(3, TEST_DATA[i].price);
            row.setField(4, TEST_DATA[i].qty);
            row.setField(5, TEST_DATA[i].date);
            res.add(row);
        }
        return res;
    }

    public static String getCreateQuery(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "id INT," +
                "title VARCHAR(50)," +
                "author VARCHAR(50)," +
                "price Double," +
                "qty INT," +
                "`date` VARCHAR(50))";
    }

    public static String getCreatePartitionQuery(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "id INT," +
                "title VARCHAR(50)," +
                "author VARCHAR(50)," +
                "price Double," +
                "qty INT," +
                "`date` VARCHAR(50)) PARTITIONED BY (author,`date`)";
    }

    public static String getInsertQuery(String tableName) {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO "+tableName+" VALUES ");
        for (int i = 0; i < TEST_DATA.length; i++) {
            sqlQueryBuilder.append("(")
                    .append(TEST_DATA[i].id).append(",'")
                    .append(TEST_DATA[i].title).append("','")
                    .append(TEST_DATA[i].author).append("',")
                    .append("cast(")
                    .append(TEST_DATA[i].price).append(" as double)").append(",")
                    .append(TEST_DATA[i].qty).append(",'")
                    .append(TEST_DATA[i].date).append("')");
            if (i < TEST_DATA.length - 1) {
                sqlQueryBuilder.append(",");
            }
        }
        return sqlQueryBuilder.toString();
    }

    public static String getInsertPartitionQuery(String tableName) {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO "+tableName+" VALUES ");
        for (int i = 0; i < TEST_DATA.length; i++) {
            sqlQueryBuilder.append("(")
                    .append(TEST_DATA[i].id).append(",'")
                    .append(TEST_DATA[i].title).append("',")
                    .append("cast(")
                    .append(TEST_DATA[i].price).append(" as double)").append(",")
                    .append(TEST_DATA[i].qty).append(",'")
                    .append(TEST_DATA[i].author).append("','")
                    .append(TEST_DATA[i].date).append("')");
            if (i < TEST_DATA.length - 1) {
                sqlQueryBuilder.append(",");
            }
        }
        return sqlQueryBuilder.toString();
    }

    public static String getInsertPartitionQuery2(String tableName) {
        return "INSERT INTO " + tableName + " SELECT id,title,price,qty,author,`date` FROM src1";
    }

    public static String getInsertPartitionQuery3(String tableName) {
        return "INSERT INTO " + tableName + " PARTITION (author = 'wsj',`date`='2020-01-01') SELECT id,title,price,qty FROM src1";
    }

    public static String getInsertPartitionQuery4(String tableName) {
        return "INSERT INTO " + tableName + " PARTITION (author = 'wsj') SELECT id,title,price,qty,`date` FROM src1";
    }

    public static String getInsertQuery2(String tableName) {
        return "INSERT INTO " + tableName + " SELECT * FROM src";
    }

    public static String SELECT_ALL_BOOKS(String tableName) {
        return "select * from " + tableName;
    }

    public static String SELECT_BOOKS_ID_TITLE(String tableName) {
        return "select title,id,id  from " + tableName;
    }

    public static String SELECT_BOOKS_TITLE_QTY(String tableName) {
        return "select title,qty,author from " + tableName;
    }

    public static String SELECT_ALL_BOOKS_SPLIT_BY_ID(String tableName) {
        return SELECT_ALL_BOOKS(tableName) + " WHERE id BETWEEN 1003 AND 1006";
    }

    public static String SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR(String tableName) {
        return SELECT_ALL_BOOKS(tableName) + " WHERE author = 'Kevin Jones'";
    }

    public static String SELECT_ID_TITLE_BOOKS_SPLIT_BY_ID(String tableName) {
        return SELECT_BOOKS_ID_TITLE(tableName) + " WHERE id BETWEEN 1003 AND 1006";
    }

    public static String SELECT_ID_TITLE_BOOKS_SPLIT_BY_AUTHOR(String tableName) {
        return SELECT_BOOKS_ID_TITLE(tableName) + " WHERE author = 'Kevin Jones'";
    }

    public static String SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED(String tableName) {
        return SELECT_BOOKS_TITLE_QTY(tableName) + " WHERE id BETWEEN 1003 AND 1006 AND author = 'Kevin Jones'";
    }

    public static String SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED2(String tableName) {
        return SELECT_BOOKS_TITLE_QTY(tableName) + " WHERE id BETWEEN 1003 AND 1006 AND author = 'Kumar' AND `date` = '2019-01-08'";
    }

    public static String SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED3(String tableName) {
        return SELECT_BOOKS_TITLE_QTY(tableName) + " WHERE id BETWEEN 1003 AND 1006 AND `date` = '2019-01-08'";
    }

    public static String SELECT_ALL_BOOKS_SPLIT_BY_ID_PARTITION_PRUNED(String tableName) {
        return SELECT_ALL_BOOKS(tableName) + " WHERE id BETWEEN 1003 AND 1006 AND `date` = '2020-01-01'";
    }

    public static String SELECT_PARTITION_COL(String tableName) {
        return "select `date`, count(`date`) from " + tableName + " group by `date`";
    }

    public static List<BookEntry> QUERY_BY_ODPS_SQL(String tableName, String sql) throws Exception {
        String odpsSqlResult = OdpsTestUtils.exec(sql +";");
        List<BookEntry> bookEntryList = new ArrayList<>();
        String lines[] = odpsSqlResult.split("\\r?\\n");
        if (lines.length==1){
            return bookEntryList;
        }
        String[] schema = lines[0].split(",");

        for(int i = 1;i< lines.length; i++) {
            String[] fields = lines[i].split(",");
            Object[] objects = new Object[fields.length];
            for(int j = 0; j < fields.length; j++){
                schema[j] = schema[j].trim().replaceAll("'", "").replaceAll("\"", "");
                fields[j] = fields[j].trim().replaceAll("'", "").replaceAll("\"", "");
                if (schema[j].equals("id") || schema[j].equals("qty")) {
                    objects[j] = Integer.valueOf(fields[j]);
                } else if (schema[j].equals("price")) {
                    objects[j] = Double.valueOf(fields[j]);
                } else {
                    objects[j] = fields[j];
                }
            }
            BookEntry bookEntry = new BookEntry();
            fillBookEntry(bookEntry, schema,objects);
            bookEntryList.add(bookEntry);
        }
        return bookEntryList;
    }


    public static String getOdpsCreateQuery(String tableName) {
        return "CREATE TABLE if not exists " + tableName + " (" +
                "id INT," +
                "title VARCHAR(50)," +
                "author VARCHAR(50)," +
                "price Double," +
                "qty INT," +
                "`date` VARCHAR(50));";
    }

    public static String getOdpsCreatePartitionQuery(String tableName) {
        return "CREATE TABLE if not exists " + tableName + " (" +
                "id INT," +
                "title VARCHAR(50)," +
                "price Double," +
                "qty INT)" +
                " PARTITIONED BY (author VARCHAR(50),`date` VARCHAR(50));";
    }

    public static String getOdpsInsertQuery(String tableName) {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
        for (int i = 0; i < TEST_DATA.length; i++) {
            sqlQueryBuilder.append("(")
                    .append(TEST_DATA[i].id).append(",'")
                    .append(TEST_DATA[i].title).append("','")
                    .append(TEST_DATA[i].author).append("',")
                    .append(TEST_DATA[i].price).append(",")
                    .append(TEST_DATA[i].qty).append(",'")
                    .append(TEST_DATA[i].date).append("')");
            if (i < TEST_DATA.length - 1) {
                sqlQueryBuilder.append(",");
            }
        }
        sqlQueryBuilder.append(";");
        return sqlQueryBuilder.toString();
    }

    public static String getOdpsInsertPartitionQuery(String table, String partitionTable) {
        StringBuilder sqlQueryBuilder = new StringBuilder("INSERT INTO " + partitionTable + " partition (author, `date`) select id,title,price,qty,author,`date` from " + table);
        sqlQueryBuilder.append(";");
        return sqlQueryBuilder.toString();
    }

    public static String dropTable(String table) {
        return "DROP TABLE IF EXISTS " + table;
    }

    public static void testSelectPartition(TableEnvironment tableEnv, String bookPartTableName) throws Exception {
        CloseableIterator<Row> iterator;
        List<BookEntry> odpsResult;
        Table t= tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID(bookPartTableName));
        iterator = t.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ALL_BOOKS_SPLIT_BY_ID(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"id","title","price","qty","author","date"});


        Table t2= tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR(bookPartTableName));
        iterator = t2.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"id","title","price","qty","author","date"});


        Table t3= tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_AUTHOR(bookPartTableName));
        iterator = t3.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ID_TITLE_BOOKS_SPLIT_BY_AUTHOR(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"title", "id", "id"});

        Table t4= tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_ID(bookPartTableName));
        iterator = t4.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ID_TITLE_BOOKS_SPLIT_BY_ID(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"title", "id", "id"});

        Table t5= tableEnv.sqlQuery(SELECT_ALL_BOOKS(bookPartTableName));
        iterator = t5.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ALL_BOOKS(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"id","title","price","qty","author","date"});

        Table t6= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED(bookPartTableName));
        iterator = t6.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"title","qty","author"});

        Table t7= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED2(bookPartTableName));
        iterator = t7.execute().collect();

        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED2(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"title","qty","author"});

        Table t8= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED3(bookPartTableName));
        iterator = t8.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED3(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"title","qty","author"});

        Table t9= tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID_PARTITION_PRUNED(bookPartTableName));
        iterator = t9.execute().collect();
        odpsResult = QUERY_BY_ODPS_SQL(bookPartTableName, SELECT_ALL_BOOKS_SPLIT_BY_ID_PARTITION_PRUNED(bookPartTableName)+";");
        compareResult(iterator, odpsResult, new String[]{"id","title","price","qty","author","date"});
    }



    public static BookEntry fillBookEntry(String[] fullColumns, Row result) throws Exception {
        BookEntry bookEntry = new BookEntry();
        Object[] objects = new Object[result.getArity()];
        for(int i = 0; i < result.getArity();i++){
            objects[i] = result.getField(i);
        }
        fillBookEntry(bookEntry, fullColumns, objects);
        return bookEntry;
    }

    public static void fillBookEntry(BookEntry bookEntry, String[] fullColumns, Object[] result) throws Exception {
        Field[] fields = bookEntry.getClass().getFields();
        Map<String, Object> columnMap = new HashMap<>();
        for (int i = 0; i < fullColumns.length; i++ ) {
            if (result[i] instanceof StringData) {
                columnMap.put(fullColumns[i], (result[i]).toString());
            } else {
                columnMap.put(fullColumns[i], result[i]);
            }
        }
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].getName();
            String columnName = fieldName.toLowerCase();
            if (!columnMap.containsKey(columnName)) {
                continue;
            }
            String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            Method method = null;
            try {
                method = bookEntry.getClass().getMethod(methodName, new Class[]{fields[i].getType()});
            } catch (NoSuchMethodException e) {
                throw new FlinkOdpsException("odps record cannot match pojo");
            }
            if (method != null) {
                method.invoke(bookEntry, new Object[]{columnMap.get(columnName)});
            }
        }
    }

    public static void compareResult(List<BookEntry> flinkRows, List<BookEntry> odpsResult) throws Exception {
        assertEquals(flinkRows.size(), odpsResult.size());
        Set<BookEntry> flinkResultSet = new HashSet<>(flinkRows);
        Set<BookEntry> odpsResultSet = new HashSet<>(odpsResult);
        Assert.assertTrue(isSetEqual(flinkResultSet, odpsResultSet));
    }

    public static boolean isSetEqual(Set<BookEntry> set1, Set<BookEntry> set2) {
        if (set1 == null && set2 == null) {
            return true; // Both are null
        }
        if (set1 == null || set2 == null || set1.size() != set2.size()) {
            return false;
        }
        Iterator<BookEntry> ite2 = set2.iterator();
        boolean isFullEqual = true;
        while (ite2.hasNext()) {
            BookEntry b = ite2.next();
            if (!set1.contains(b)) {
                isFullEqual = false;
            }
        }
        return isFullEqual;
    }

    public static <T> List<T> copyIterator(Iterator<Tuple2<Boolean,T>> iter) {
        List<T> copy = new ArrayList<T>();
        while (iter.hasNext())
            copy.add(iter.next().getField(1));
        return copy;
    }

    public void verifyWrittenData(List<Row> expected, List<Row> results) throws Exception {
        assertEquals(expected.size(), results.size());
        Set<String> expectedSet = new HashSet<>();
        Set<String> resultSet = new HashSet<>();
        for (int i = 0; i < results.size(); i++) {
            expectedSet.add(expected.get(i).toString());
            resultSet.add(results.get(i).toString());
        }
        assertEquals(expectedSet, resultSet);
    }

    public static void compareResult(CloseableIterator<Row> flinkIter, List<BookEntry> odpsResult, String[] fullColumns) {
        List<Row> copy = new ArrayList<Row>();
        while (flinkIter.hasNext())
            copy.add(flinkIter.next());
        compareResult(copy, odpsResult, fullColumns);
    }

    public static void compareResult(Iterator<Tuple2<Boolean, Row>> flinkIter, List<BookEntry> odpsResult, String[] fullColumns) {
        List<Row> flinkRows = copyIterator(flinkIter);
        compareResult(flinkRows, odpsResult, fullColumns);
    }

    public static void compareResult(List<Row> flinkRows, List<BookEntry> odpsResult, String[] fullColumns) {
        Set<BookEntry> flinkResultSet = flinkRows.stream().map(row -> {
            try {
                return fillBookEntry(fullColumns, row);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toSet());
        Set<BookEntry> odpsResultSet = new HashSet<>(odpsResult);
        Assert.assertTrue(isSetEqual(flinkResultSet, odpsResultSet));
    }

    public static void compareRowDataResult(List<GenericRowData> flinkRows, List<BookEntry> odpsResult, String[] fullColumns) {
        Set<BookEntry> flinkResultSet = flinkRows.stream().map(row -> {
            try {
                return fillBookEntry(fullColumns, row);
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toSet());
        Set<BookEntry> odpsResultSet = new HashSet<>(odpsResult);
        Assert.assertTrue(isSetEqual(flinkResultSet, odpsResultSet));
    }

    public static BookEntry fillBookEntry(String[] fullColumns, GenericRowData result) throws Exception {
        BookEntry bookEntry = new BookEntry();
        Object[] objects = new Object[result.getArity()];
        for(int i = 0; i < result.getArity();i++){
            objects[i] = result.getField(i);
        }
        fillBookEntry(bookEntry, fullColumns, objects);
        return bookEntry;
    }
}
