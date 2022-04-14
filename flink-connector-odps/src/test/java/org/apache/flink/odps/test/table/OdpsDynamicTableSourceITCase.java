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

import org.apache.flink.odps.test.catalog.OdpsCatalogUtils;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.odps.test.util.OdpsTestUtils.dropOdpsTable;
import static org.junit.Assert.*;

public class OdpsDynamicTableSourceITCase {

    public static StreamExecutionEnvironment env;
    public static TableEnvironment tEnv;
    public static final String INPUT_TABLE = "odpsDynamicTableSource";
    public static final String INPUT_PART_TABLE = "odpsDynamicPartitionTableSource";
    public static final String INPUT_PART2_TABLE = "odpsDynamicPartition2TableSource";
    public static final String TABLE_PRIMITIVE_TYPE = "primitiveTable";
    public static final String TABLE_ARRAY_TYPE = "arrayTable";

    public static OdpsConf odpsConf;

    private static String getCreateOdpsSourceQuery() {
        return "CREATE TABLE if not exists " + INPUT_TABLE + " ("
                + "id BIGINT NOT NULL,"
                + "timestamp3_col DATETIME, "
                + "timestamp9_col TIMESTAMP, "
                + "string_col STRING, "
                + "double_col DOUBLE,"
                + "decimal_col DECIMAL(10, 4));";
    }

    private static String getInsertOdpsSourceQuery() {
        return "INSERT INTO "
                + INPUT_TABLE
                + " VALUES (1, DATETIME('2020-01-01 15:35:01'), TIMESTAMP('2020-01-01 15:35:00.123456789'), 'string1', 1.79769E+308, 100.1234), "
                + "(2, DATETIME('2020-01-01 15:36:01'), TIMESTAMP('2020-01-01 15:36:01.123456789'), 'string2', -1.79769E+308, 101.1234);";
    }

    private static String getCreateOdpsPart1SourceQuery() {
        return "CREATE TABLE if not exists " + INPUT_PART_TABLE
                + " (`year` STRING, `value` STRING) partitioned by (pt STRING);";
    }

    private static String getInsertOdpsPart1SourceQuery() {
        return "INSERT INTO "
                + INPUT_PART_TABLE
                + " partition (pt) (`year`, `value`, pt) VALUES ('2015', '2', '1'),('2015', '5', '1'),('2014', '3', '0'),('2014', '4', '0');";
    }

    private static String getCreateOdpsPart2SourceQuery() {
        return "CREATE TABLE if not exists " + INPUT_PART2_TABLE
                + " (x string) partitioned by (p1 int,p2 string);";
    }

    private static String getInsertOdpsPart2SourceQuery() {
        return "INSERT INTO "
                + INPUT_PART2_TABLE
                + " partition (p1, p2) (x, p1, p2) VALUES ('1', 1, 'a'),('2', 2, 'b'),('3', 3, 'c'),('4', 4, 'd');";
    }

    @BeforeClass
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        odpsConf = OdpsTestUtils.getOdpsConf();
        OdpsTestUtils.exec(dropOdpsTable(INPUT_TABLE));
        OdpsTestUtils.exec(getCreateOdpsSourceQuery());
        OdpsTestUtils.exec(getInsertOdpsSourceQuery());

        OdpsTestUtils.exec(dropOdpsTable(INPUT_PART_TABLE));
        OdpsTestUtils.exec(getCreateOdpsPart1SourceQuery());
        OdpsTestUtils.exec(getInsertOdpsPart1SourceQuery());
    }

    @After
    public void clearOutputTable() throws Exception {

    }

    @Test
    public void testOdpsSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "timestamp3_col TIMESTAMP(3),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "string_col STRING,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + INPUT_TABLE
                        + "'"
                        + ")");

        Iterator<Row> collected = tEnv.executeSql("SELECT * FROM " + INPUT_TABLE).collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                        "+I[1, 2020-01-01T15:35:01, 2020-01-01T15:35:00.123456789, string1, 1.79769E308, 100.1234]",
                        "+I[2, 2020-01-01T15:36:01, 2020-01-01T15:36:01.123456789, string2, -1.79769E308, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testProject() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_TABLE
                        + "("
                        + "id BIGINT,"
                        + "timestamp3_col TIMESTAMP(3),"
                        + "timestamp9_col TIMESTAMP(9),"
                        + "string_col STRING,"
                        + "double_col DOUBLE,"
                        + "decimal_col DECIMAL(10, 4)"
                        + ") WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + INPUT_TABLE
                        + "'"
                        + ")");

        Iterator<Row> collected =
                tEnv.executeSql("SELECT id,timestamp9_col,decimal_col FROM " + INPUT_TABLE)
                        .collect();
        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        List<String> expected =
                Stream.of(
                        "+I[1, 2020-01-01T15:35:00.123456789, 100.1234]",
                        "+I[2, 2020-01-01T15:36:01.123456789, 101.1234]")
                        .sorted()
                        .collect(Collectors.toList());
        assertEquals(expected, result);
    }

    @Test
    public void testReadOdpsPartitionSource() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_PART_TABLE
                        + "(`year` STRING, `value` STRING, pt STRING) partitioned by (pt) WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + INPUT_PART_TABLE
                        + "'"
                        + ")");

        Table src = tEnv.sqlQuery("select * from " + INPUT_PART_TABLE);
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertEquals(4, rows.size());
        Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
        assertArrayEquals(
                new String[] {
                        "+I[2014, 3, 0]", "+I[2014, 4, 0]", "+I[2015, 2, 1]", "+I[2015, 5, 1]"
                },
                rowStrings);
    }

    @Test
    public void testPartitionPrunning() throws Exception {
        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_PART_TABLE
                        + "(`year` STRING, `value` STRING, pt STRING) partitioned by (pt) WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + INPUT_PART_TABLE
                        + "'"
                        + ")");
        Table src =
                tEnv.sqlQuery("select * from "+ INPUT_PART_TABLE + " where pt = '0'");
        // first check execution plan to ensure partition prunning works
        String[] explain = src.explain().split("==.*==\n");
        assertEquals(4, explain.length);
        String optimizedLogicalPlan = explain[2];
        assertTrue(
                optimizedLogicalPlan,
                optimizedLogicalPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartitionTableSource, partitions=[{pt=0}], project=[year, value]]]"));
        List<Row> rows = CollectionUtil.iteratorToList(src.execute().collect());
        assertEquals(2, rows.size());
        Object[] rowStrings = rows.stream().map(Row::toString).sorted().toArray();
        assertArrayEquals(new String[] {"+I[2014, 3, 0]", "+I[2014, 4, 0]"}, rowStrings);
    }

    @Test
    public void testPartitionFilter() {
        tEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        OdpsTestUtils.exec(dropOdpsTable(INPUT_PART2_TABLE));
        OdpsTestUtils.exec(getCreateOdpsPart2SourceQuery());
        OdpsTestUtils.exec(getInsertOdpsPart2SourceQuery());

        tEnv.executeSql(
                "CREATE TABLE "
                        + INPUT_PART2_TABLE
                        + "(x STRING, p1 INT, p2 STRING) partitioned by (p1, p2) WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + INPUT_PART2_TABLE
                        + "'"
                        + ")");
        Table query =
                tEnv.sqlQuery("select x from " + INPUT_PART2_TABLE + " where p1>1 or p2<>'a' order by x");
        String[] explain = query.explain().split("==.*==\n");
        String optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[{p1=2, p2=b}, {p1=3, p2=c}, {p1=4, p2=d}]"));
        List<Row> results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[+I[2], +I[3], +I[4]]", results.toString());

        query = tEnv.sqlQuery("select x from " + INPUT_PART2_TABLE + " where p1>2 and p2<='a' order by x");
        explain = query.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[], project=[x]]]"));
        results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[]", results.toString());

        query = tEnv.sqlQuery("select x from " + INPUT_PART2_TABLE + " where p1 in (1,3,5) order by x");
        explain = query.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[{p1=1, p2=a}, {p1=3, p2=c}], project=[x]]]"));
        results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[+I[1], +I[3]]", results.toString());

        query =
                tEnv.sqlQuery(
                        "select x from " + INPUT_PART2_TABLE + " where (p1=1 and p2='a') or ((p1=2 and p2='b') or p2='d') order by x");
        explain = query.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[{p1=1, p2=a}, {p1=2, p2=b}, {p1=4, p2=d}], project=[x]]]"));
        results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[+I[1], +I[2], +I[4]]", results.toString());

        query = tEnv.sqlQuery("select x from " + INPUT_PART2_TABLE + " where p2 = 'd' order by x");
        explain = query.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[{p1=4, p2=d}], project=[x]]]"));
        results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[+I[4]]", results.toString());

        query = tEnv.sqlQuery("select x from " + INPUT_PART2_TABLE + " where '' = p2");
        explain = query.explain().split("==.*==\n");
        optimizedPlan = explain[2];
        assertTrue(
                optimizedPlan,
                optimizedPlan.contains(
                        "table=[[default_catalog, default_database, odpsDynamicPartition2TableSource, partitions=[], project=[x]]]"));
        results = CollectionUtil.iteratorToList(query.execute().collect());
        assertEquals("[]", results.toString());
    }

    @Test
    public void testSelectPrimitiveType() throws Exception {
        tEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        TestTable table = getPrimitiveTable();
        tEnv.executeSql(
                "CREATE TABLE "
                        + TABLE_PRIMITIVE_TYPE
                        + "("
                        + table.flinkSchemaSql
                        + ") WITH ("
                        + "  'connector'='odps',"
                        + "  'odps.access.id'='"
                        + odpsConf.getAccessId()
                        + "',"
                        + "  'odps.access.key'='"
                        + odpsConf.getAccessKey()
                        + "',"
                        + "  'odps.end.point'='"
                        + odpsConf.getEndpoint()
                        + "',"
                        + "  'odps.project.name'='"
                        + odpsConf.getProject()
                        + "',"
                        + "  'table-name'='"
                        + odpsConf.getProject()
                        + "."
                        + TABLE_PRIMITIVE_TYPE
                        + "'"
                        + ")");

        OdpsTestUtils.exec(dropOdpsTable(TABLE_PRIMITIVE_TYPE));

        OdpsTestUtils.exec(String.format(
                "CREATE TABLE if not exists %s (%s);",
                TABLE_PRIMITIVE_TYPE, table.odpsSchemaSql));

        OdpsTestUtils.exec(String.format(
                "insert into %s values (%s);",
                TABLE_PRIMITIVE_TYPE, table.values));

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from `%s`", TABLE_PRIMITIVE_TYPE))
                                .execute()
                                .collect());
        assertEquals(
                "[+I[1, 2, 3, 4, 5.5, 6.6, 7.70000, true, a, [50], b  , c, 2016-06-22T19:10:25, 2015-01-01, 2017-11-11T00:00]]",
                results.toString());
    }


    @Test
    public void testSelectArrayTypes() throws Exception {
        TestTable table = getArrayTable();
        tEnv.executeSql("CREATE TABLE "
                + TABLE_ARRAY_TYPE
                + "("
                + table.flinkSchemaSql
                + ") WITH ("
                + "  'connector'='odps',"
                + "  'odps.access.id'='"
                + odpsConf.getAccessId()
                + "',"
                + "  'odps.access.key'='"
                + odpsConf.getAccessKey()
                + "',"
                + "  'odps.end.point'='"
                + odpsConf.getEndpoint()
                + "',"
                + "  'odps.project.name'='"
                + odpsConf.getProject()
                + "',"
                + "  'table-name'='"
                + odpsConf.getProject()
                + "."
                + TABLE_ARRAY_TYPE
                + "'"
                + ")");
        OdpsTestUtils.exec(dropOdpsTable(TABLE_ARRAY_TYPE));

        OdpsTestUtils.exec(String.format(
                "CREATE TABLE if not exists %s (%s);",
                TABLE_ARRAY_TYPE, table.odpsSchemaSql));

        OdpsTestUtils.exec(String.format(
                "insert into %s values (%s);",
                TABLE_ARRAY_TYPE, table.values));

        List<Row> results =
                CollectionUtil.iteratorToList(
                        tEnv.sqlQuery(String.format("select * from %s", TABLE_ARRAY_TYPE))
                                .execute()
                                .collect());
        assertEquals(
                "[+I["
                        + "[1, 2, 3], "
                        + "[[50], [51], [52]], "
                        + "[3, 4, 5], "
                        + "[4, 5, 6], "
                        + "[5.5, 6.6, 7.7], "
                        + "[6.6, 7.7, 8.8], "
                        + "[7.70000, 8.80000, 9.90000], "
                        + "[8.800000000000000000, 9.900000000000000000, 10.100000000000000000], "
                        + "[true, false, true], "
                        + "[a, b, c], "
                        + "[b  , c  , d  ], "
                        + "[b, c, d], "
                        + "[2016-06-22T19:10:25, 2019-06-22T19:10:25], "
                        + "[2015-01-01, 2020-01-01], "
                        + "null, "
                        + "null]]",
                results.toString());
    }


    public static class TestTable {
        TableSchema schema;
        String flinkSchemaSql;
        String odpsSchemaSql;
        String values;

        public TestTable(TableSchema schema, String flinkSchemaSql, String odpsSchemaSql, String values) {
            this.schema = schema;
            this.flinkSchemaSql = flinkSchemaSql;
            this.odpsSchemaSql = odpsSchemaSql;
            this.values = values;
        }
    }

    public static TestTable getPrimitiveTable() {
        return new TestTable(
                TableSchema.builder()
                        .field("d_int", DataTypes.INT().notNull())
                        .field("d_tinyint", DataTypes.TINYINT())
                        .field("d_short", DataTypes.SMALLINT().notNull())
                        .field("d_long", DataTypes.BIGINT())
                        .field("d_real", DataTypes.FLOAT())
                        .field("d_double_precision", DataTypes.DOUBLE())
                        .field("d_decimal", DataTypes.DECIMAL(10, 5))
                        .field("d_boolean", DataTypes.BOOLEAN())
                        .field("d_string", DataTypes.STRING())
                        .field("d_bytes", DataTypes.BYTES())
                        .field("d_char", DataTypes.CHAR(3))
                        .field("d_vchar", DataTypes.VARCHAR(20))
                        .field("d_timestamp", DataTypes.TIMESTAMP(9))
                        .field("d_date", DataTypes.DATE())
                        .field("d_datatime", DataTypes.TIMESTAMP(3))
                        .build(),
                "d_int int, "
                        + "d_tinyint tinyint, "
                        + "d_short smallint, "
                        + "d_long bigint, "
                        + "d_real float, "
                        + "d_double_precision double, "
                        + "d_decimal decimal(10, 5), "
                        + "d_boolean boolean, "
                        + "d_string string, "
                        + "d_bytes bytes, "
                        + "d_char char(3), "
                        + "d_vchar varchar(20), "
                        + "d_timestamp timestamp(9), "
                        + "d_date date,"
                        + "d_datatime timestamp(3)",
                "d_int int, "
                        + "d_tinyint tinyint, "
                        + "d_short smallint, "
                        + "d_long bigint, "
                        + "d_real float, "
                        + "d_double_precision double, "
                        + "d_decimal decimal(10, 5), "
                        + "d_boolean boolean, "
                        + "d_string string, "
                        + "d_bytes binary, "
                        + "d_char char(3), "
                        + "d_vchar varchar(20), "
                        + "d_timestamp timestamp, "
                        + "d_date date,"
                        + "d_datatime datetime",
                "1,"
                        + "cast('2' as tinyint),"
                        + "cast('3' as smallint),"
                        + "4,"
                        + "cast('5.5' as float),"
                        + "6.6,"
                        + "7.7,"
                        + "true,"
                        + "'a',"
                        + "cast('2' as binary),"
                        + "'b',"
                        + "'c',"
                        + "timestamp'2016-06-22 19:10:25',"
                        + "date'2015-01-01',"
                        + "DATETIME'2017-11-11 00:00:00'");
    }

    public static TestTable getArrayTable() {
        return new TestTable(
                TableSchema.builder()
                        .field("int_arr", DataTypes.ARRAY(DataTypes.INT()))
                        .field("bytea_arr", DataTypes.ARRAY(DataTypes.BYTES()))
                        .field("short_arr", DataTypes.ARRAY(DataTypes.SMALLINT()))
                        .field("long_arr", DataTypes.ARRAY(DataTypes.BIGINT()))
                        .field("real_arr", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .field("double_precision_arr", DataTypes.ARRAY(DataTypes.DOUBLE()))
                        .field("numeric_arr", DataTypes.ARRAY(DataTypes.DECIMAL(10, 5)))
                        .field(
                                "numeric_arr_default",
                                DataTypes.ARRAY(DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18)))
                        .field("boolean_arr", DataTypes.ARRAY(DataTypes.BOOLEAN()))
                        .field("text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .field("character_arr", DataTypes.ARRAY(DataTypes.CHAR(3)))
                        .field("character_varying_arr", DataTypes.ARRAY(DataTypes.VARCHAR(20)))
                        .field("timestamp_arr", DataTypes.ARRAY(DataTypes.TIMESTAMP(5)))
                        .field("date_arr", DataTypes.ARRAY(DataTypes.DATE()))
                        .field("null_bytea_arr", DataTypes.ARRAY(DataTypes.BYTES()))
                        .field("null_text_arr", DataTypes.ARRAY(DataTypes.STRING()))
                        .build(),
                "int_arr array<int>, "
                        + "bytea_arr array<binary>, "
                        + "short_arr array<smallint>, "
                        + "long_arr array<bigint>, "
                        + "real_arr array<float>, "
                        + "double_precision_arr array<double>, "
                        + "numeric_arr array<decimal(10, 5)>, "
                        + "numeric_arr_default array<decimal(38,18)>, "
                        + "boolean_arr array<boolean>, "
                        + "text_arr array<string>, "
                        + "character_arr array<char(3)>, "
                        + "character_varying_arr array<varchar(20)>, "
                        + "timestamp_arr array<timestamp(9)>, "
                        + "date_arr array<date>, "
                        + "null_bytea_arr array<bytes>, "
                        + "null_text_arr array<string>",
                "int_arr array<int>, "
                        + "bytea_arr array<binary>, "
                        + "short_arr array<smallint>, "
                        + "long_arr array<bigint>, "
                        + "real_arr array<float>, "
                        + "double_precision_arr array<double>, "
                        + "numeric_arr array<decimal(10, 5)>, "
                        + "numeric_arr_default array<decimal(38,18)>, "
                        + "boolean_arr array<boolean>, "
                        + "text_arr array<string>, "
                        + "character_arr array<char(3)>, "
                        + "character_varying_arr array<varchar(20)>, "
                        + "timestamp_arr array<timestamp>, "
                        + "date_arr array<date>, "
                        + "null_bytea_arr array<binary>, "
                        + "null_text_arr array<string>",
                String.format(
                        "array(1,2,3),"
                                + "array(cast('2' as binary),cast('3' as binary),cast('4' as binary)),"
                                + "array(cast('3' as smallint) ,cast('4' as smallint), cast('5' as smallint)),"
                                + "array(4,5,6),"
                                + "array(cast('5.5' as float),cast('6.6' as float),cast('7.7' as float)),"
                                + "array(6.6,7.7,8.8),"
                                + "array(7.7,8.8,9.9),"
                                + "array(cast('8.8' as decimal(38,18)),9.9,10.10),"
                                + "array(true,false,true),"
                                + "array('a','b','c'),"
                                + "array('b','c','d'),"
                                + "array('b','c','d'),"
                                + "array(timestamp'2016-06-22 19:10:25', timestamp'2019-06-22 19:10:25'),"
                                + "array(date'2015-01-01', date'2020-01-01'),"
                                + "NULL,"
                                + "NULL"));
    }
}
