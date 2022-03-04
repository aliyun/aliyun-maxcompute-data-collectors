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

import org.apache.flink.odps.catalog.OdpsCatalog;
import org.apache.flink.odps.test.catalog.OdpsCatalogUtils;
import org.apache.flink.odps.test.util.BookTableUtils;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.odps.test.util.BookTableUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OdpsTableFactoryWithCatalogTest {

    private static TableEnvironment tableEnv;
    private static OdpsCatalog odpsCatalog;
    private static String table;
    private static String partitionTable;

    @BeforeClass
    public static void setup() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        odpsCatalog = OdpsCatalogUtils.createOdpsCatalog();
        odpsCatalog.open();
        tableEnv.registerCatalog(odpsCatalog.getName(), odpsCatalog);
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
    public void testSelectPrimaryTable() throws Exception {
        List<String> result;
        List<String> expected;

        tableEnv.useCatalog(odpsCatalog.getName());
        Table t = tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID(table));
        result =
                CollectionUtil.iteratorToList(t.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[1003, More Java for more dummies, Mohammad Ali, 33.33, 33, 2019-01-08]",
                        "+I[1004, A Cup of Java, Kumar, 44.44, 44, 2019-01-08]",
                        "+I[1005, A Teaspoon of Java, Kevin Jones, 55.55, 55, 2019-02-09]",
                        "+I[1006, A Teaspoon of Java 1.4, Kevin Jones, 66.66, 66, 2019-02-09]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);

        Table t2 = tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR(table));
        result =
                CollectionUtil.iteratorToList(t2.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[1005, A Teaspoon of Java, Kevin Jones, 55.55, 55, 2019-02-09]",
                        "+I[1006, A Teaspoon of Java 1.4, Kevin Jones, 66.66, 66, 2019-02-09]",
                        "+I[1007, A Teaspoon of Java 1.5, Kevin Jones, 77.77, 77, 2019-02-07]",
                        "+I[1008, A Teaspoon of Java 1.6, Kevin Jones, 88.88, 88, 2019-02-07]",
                        "+I[1009, A Teaspoon of Java 1.7, Kevin Jones, 99.99, 99, 2019-02-08]",
                        "+I[1010, A Teaspoon of Java 1.8, Kevin Jones, 100.0, 1010, 2019-02-08]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);

        Table t3 = tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_AUTHOR(table));
        result =
                CollectionUtil.iteratorToList(t3.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[A Teaspoon of Java 1.4, 1006, 1006]",
                        "+I[A Teaspoon of Java 1.5, 1007, 1007]",
                        "+I[A Teaspoon of Java 1.6, 1008, 1008]",
                        "+I[A Teaspoon of Java 1.7, 1009, 1009]",
                        "+I[A Teaspoon of Java 1.8, 1010, 1010]",
                        "+I[A Teaspoon of Java, 1005, 1005]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);

        Table t4= tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_ID(table));
        result =
                CollectionUtil.iteratorToList(t4.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[More Java for more dummies, 1003, 1003]",
                        "+I[A Cup of Java, 1004, 1004]",
                        "+I[A Teaspoon of Java, 1005, 1005]",
                        "+I[A Teaspoon of Java 1.4, 1006, 1006]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testSelectPartition() throws Exception {
        List<String> result;
        List<String> expected;
        String[] explain;
        String optimizedPlan;

        tableEnv.useCatalog(odpsCatalog.getName());
        Table t = tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID(partitionTable));
        result =
                CollectionUtil.iteratorToList(t.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[1003, More Java for more dummies, 33.33, 33, Mohammad Ali, 2019-01-08]",
                        "+I[1004, A Cup of Java, 44.44, 44, Kumar, 2019-01-08]",
                        "+I[1005, A Teaspoon of Java, 55.55, 55, Kevin Jones, 2019-02-09]",
                        "+I[1006, A Teaspoon of Java 1.4, 66.66, 66, Kevin Jones, 2019-02-09]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
        explain = t.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "fields=[id, title, price, qty, author, date]"));

        Table t2= tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_AUTHOR(partitionTable));
        result =
                CollectionUtil.iteratorToList(t2.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[1005, A Teaspoon of Java, 55.55, 55, Kevin Jones, 2019-02-09]",
                        "+I[1006, A Teaspoon of Java 1.4, 66.66, 66, Kevin Jones, 2019-02-09]",
                        "+I[1007, A Teaspoon of Java 1.5, 77.77, 77, Kevin Jones, 2019-02-07]",
                        "+I[1008, A Teaspoon of Java 1.6, 88.88, 88, Kevin Jones, 2019-02-07]",
                        "+I[1009, A Teaspoon of Java 1.7, 99.99, 99, Kevin Jones, 2019-02-08]",
                        "+I[1010, A Teaspoon of Java 1.8, 100.0, 1010, Kevin Jones, 2019-02-08]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
        explain = t2.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[{author=Kevin Jones, date=2019-02-07}, {author=Kevin Jones, date=2019-02-08}, {author=Kevin Jones, date=2019-02-09}], project=[id, title, price, qty, date]]], fields=[id, title, price, qty, date]"));

        Table t3= tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_AUTHOR(partitionTable));
        result =
                CollectionUtil.iteratorToList(t3.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[A Teaspoon of Java 1.4, 1006, 1006]",
                        "+I[A Teaspoon of Java 1.5, 1007, 1007]",
                        "+I[A Teaspoon of Java 1.6, 1008, 1008]",
                        "+I[A Teaspoon of Java 1.7, 1009, 1009]",
                        "+I[A Teaspoon of Java 1.8, 1010, 1010]",
                        "+I[A Teaspoon of Java, 1005, 1005]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
        explain = t3.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[{author=Kevin Jones, date=2019-02-07}, {author=Kevin Jones, date=2019-02-08}, {author=Kevin Jones, date=2019-02-09}], project=[title, id]]], fields=[title, id]"));

        Table t4= tableEnv.sqlQuery(SELECT_ID_TITLE_BOOKS_SPLIT_BY_ID(partitionTable));
        result =
                CollectionUtil.iteratorToList(t4.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[More Java for more dummies, 1003, 1003]",
                        "+I[A Cup of Java, 1004, 1004]",
                        "+I[A Teaspoon of Java, 1005, 1005]",
                        "+I[A Teaspoon of Java 1.4, 1006, 1006]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
        explain = t4.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "project=[id, title]]], fields=[id, title]"));

        Table t5= tableEnv.sqlQuery(SELECT_ALL_BOOKS(partitionTable));
        result =
                CollectionUtil.iteratorToList(t5.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        expected =
                Stream.of(
                        "+I[1007, A Teaspoon of Java 1.5, 77.77, 77, Kevin Jones, 2019-02-07]",
                        "+I[1008, A Teaspoon of Java 1.6, 88.88, 88, Kevin Jones, 2019-02-07]",
                        "+I[1009, A Teaspoon of Java 1.7, 99.99, 99, Kevin Jones, 2019-02-08]",
                        "+I[1010, A Teaspoon of Java 1.8, 100.0, 1010, Kevin Jones, 2019-02-08]",
                        "+I[1005, A Teaspoon of Java, 55.55, 55, Kevin Jones, 2019-02-09]",
                        "+I[1006, A Teaspoon of Java 1.4, 66.66, 66, Kevin Jones, 2019-02-09]",
                        "+I[1004, A Cup of Java, 44.44, 44, Kumar, 2019-01-08]",
                        "+I[1003, More Java for more dummies, 33.33, 33, Mohammad Ali, 2019-01-08]",
                        "+I[1002, More Java for dummies, 22.22, 22, Tan Ah Teck, 2019-01-08]",
                        "+I[1001, Java public for dummies, 11.11, 11, Tan Ah Teck, 2019-01-09]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
        explain = t5.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "fields=[id, title, price, qty, author, date]"));

        Table t6= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED(partitionTable));
        result =
                CollectionUtil.iteratorToList(t6.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        explain = t6.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[{author=Kevin Jones, date=2019-02-07}, {author=Kevin Jones, date=2019-02-08}, {author=Kevin Jones, date=2019-02-09}], project=[id, title, qty, author]]], fields=[id, title, qty, author]"));
        expected =
                Stream.of(
                        "+I[A Teaspoon of Java 1.4, 66, Kevin Jones]",
                        "+I[A Teaspoon of Java, 55, Kevin Jones]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);


        Table t7= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED2(partitionTable));
        result =
                CollectionUtil.iteratorToList(t7.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        explain = t7.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[{author=Kumar, date=2019-01-08}], project=[id, title, qty, author]]], fields=[id, title, qty, author]"));
        expected =
                Stream.of(
                        "+I[A Cup of Java, 44, Kumar]")
                        .collect(Collectors.toList());
        assertEquals(expected, result);

        Table t8= tableEnv.sqlQuery(SELECT_BOOKS_TITLE_QTY_SPLIT_BY_ID_PARTITION_PRUNED3(partitionTable));
        result =
                CollectionUtil.iteratorToList(t8.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        explain = t8.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[{author=Kumar, date=2019-01-08}, {author=Mohammad Ali, date=2019-01-08}, {author=Tan Ah Teck, date=2019-01-08}], project=[id, title, qty, author]]], fields=[id, title, qty, author]"));
        expected =
                Stream.of(
                        "+I[A Cup of Java, 44, Kumar]",
                        "+I[More Java for more dummies, 33, Mohammad Ali]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);

        Table t9= tableEnv.sqlQuery(SELECT_ALL_BOOKS_SPLIT_BY_ID_PARTITION_PRUNED(partitionTable));
        result =
                CollectionUtil.iteratorToList(t9.execute().collect()).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        explain = t9.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "partitions=[]]], fields=[id, title, price, qty, author, date]"));
        expected = new ArrayList<>();
        assertEquals(expected, result);
    }
}
