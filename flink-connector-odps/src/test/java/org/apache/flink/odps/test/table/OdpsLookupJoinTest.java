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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.odps.input.OdpsInputFormat;
import org.apache.flink.odps.input.OdpsLookupFunction;
import org.apache.flink.odps.input.OdpsLookupOptions;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Collector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

public class OdpsLookupJoinTest {

    private static final String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static final DataType[] fieldDataTypes =
            new DataType[] {
                    DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };
    private static final int[] lookupKeys = new int[] {0, 1};
    private static final String LOOKUP_TABLE = "lookup_table";

    private static OdpsConf odpsConf;

    @BeforeClass
    public static void before() {
        odpsConf = OdpsTestUtils.getOdpsConf();

        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + LOOKUP_TABLE
                        + " ("
                        + "id1 INT NOT NULL,"
                        + "id2 STRING NOT NULL,"
                        + "comment1 STRING,"
                        + "comment2 STRING);");

        Object[][] data =
                new Object[][] {
                        new Object[] {1, "1", "11-c1-v1", "11-c2-v1"},
                        new Object[] {1, "1", "11-c1-v2", "11-c2-v2"},
                        new Object[] {2, "3", null, "23-c2"},
                        new Object[] {2, "5", "25-c1", "25-c2"},
                        new Object[] {3, "8", "38-c1", "38-c2"}
                };

        boolean[] surroundedByQuotes = new boolean[] {false, true, true, true};

        StringBuilder sqlQueryBuilder =
                new StringBuilder(
                        "INSERT INTO "
                                + LOOKUP_TABLE
                                + " (id1, id2, comment1, comment2) VALUES ");
        for (int i = 0; i < data.length; i++) {
            sqlQueryBuilder.append("(");
            for (int j = 0; j < data[i].length; j++) {
                if (data[i][j] == null) {
                    sqlQueryBuilder.append("null");
                } else {
                    if (surroundedByQuotes[j]) {
                        sqlQueryBuilder.append("'");
                    }
                    sqlQueryBuilder.append(data[i][j]);
                    if (surroundedByQuotes[j]) {
                        sqlQueryBuilder.append("'");
                    }
                }
                if (j < data[i].length - 1) {
                    sqlQueryBuilder.append(", ");
                }
            }
            sqlQueryBuilder.append(")");
            if (i < data.length - 1) {
                sqlQueryBuilder.append(", ");
            }
        }

        OdpsTestUtils.exec(sqlQueryBuilder.toString() + ";");
    }

    @AfterClass
    public static void clearOutputTable() throws Exception {
        OdpsTestUtils.exec("DROP TABLE " + LOOKUP_TABLE + ";");
    }

    @Test
    public void testLookupFunction() throws Exception {

        OdpsLookupFunction lookupFunction = buildOdpsLookupFunction();
        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(1, StringData.fromString("1"));

        lookupFunction.eval(2, StringData.fromString("3"));

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        Collections.sort(expected);

        assertEquals(expected, result);
    }

    @Test
    public void testLookupTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "3"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "5"),
                                        new Tuple2<>(3, "8"))),
                        $("id1"),
                        $("id2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);
        String cacheConfig = ", 'lookup.cache.max-rows'='4', 'lookup.cache.ttl'='10000'";
        tEnv.executeSql(
                "CREATE TABLE "
                        + "lookup ("
                        + "  id1 INT,"
                        + "  comment1 STRING,"
                        + "  comment2 STRING,"
                        + "  id2 STRING"
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
                        + LOOKUP_TABLE
                        + "'"
                        + ")");

        String sqlQuery =
                "SELECT source.id1, source.id2, L.comment1, L.comment2 FROM T AS source "
                        + "JOIN lookup for system_time as of source.proctime AS L "
                        + "ON source.id1 = L.id1 and source.id2 = L.id2";
        Iterator<Row> collected = tEnv.executeSql(sqlQuery).collect();

        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I[1, 1, 11-c1-v1, 11-c2-v1]");
        expected.add("+I[1, 1, 11-c1-v1, 11-c2-v1]");
        expected.add("+I[1, 1, 11-c1-v2, 11-c2-v2]");
        expected.add("+I[1, 1, 11-c1-v2, 11-c2-v2]");
        expected.add("+I[2, 3, null, 23-c2]");
        expected.add("+I[2, 5, 25-c1, 25-c2]");
        expected.add("+I[3, 8, 38-c1, 38-c2]");
        Collections.sort(expected);
        assertEquals(expected, result);
    }

    private OdpsLookupFunction buildOdpsLookupFunction() {
        OdpsLookupOptions lookupOptions = OdpsLookupOptions.builder().build();
        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);
        OdpsInputFormat.OdpsInputFormatBuilder<RowData> builder =
                new OdpsInputFormat.OdpsInputFormatBuilder<>(
                        odpsConf,
                        odpsConf.getProject(),
                        LOOKUP_TABLE);
        return new OdpsLookupFunction(
                builder.build(),
                lookupOptions,
                lookupKeys,
                rowType);
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
