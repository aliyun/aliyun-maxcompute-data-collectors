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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.odps.test.catalog.OdpsCatalogUtils;
import org.apache.flink.odps.test.util.OdpsTestUtils;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.odps.test.util.OdpsTestUtils.QUERY_BY_ODPS_SQL;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.util.ExceptionUtils.findThrowableWithMessage;
import static org.junit.Assert.assertTrue;

public class OdpsDynamicTableSinkITCase {
    public static final String OUTPUT_TABLE = "ODPS_DATAGEN";
    public static final String CREATE_DATAGEN_TABLE =
            "CREATE TABLE datagen_t (\n"
                    + "	f1 STRING,\n"
                    + "	f2 BOOLEAN,\n"
                    + "	f3 DECIMAL(32,2),\n"
                    + "	f4 TINYINT,\n"
                    + "	f5 SMALLINT,\n"
                    + "	f6 INT,\n"
                    + "	f7 BIGINT,\n"
                    + "	f8 FLOAT,\n"
                    + "	f9 DOUBLE,\n"
                    + "	f10 DATE,\n"
                    + "	f11 TIMESTAMP(3),\n"
                    + "	f12 TIMESTAMP(9)\n"
                    + ") WITH ("
                    + "	'connector' = 'datagen',\n"
                    + "	'number-of-rows' = '100000'"
                    + ")";
    public static final String CREATE_ODPS_DATA_GEN_TABLE =
            "CREATE TABLE "
            + OUTPUT_TABLE
            + " ("
            + "	f1 STRING,\n"
            + "	f2 BOOLEAN,\n"
            + "	f3 DECIMAL(32,2),\n"
            + "	f4 TINYINT,\n"
            + "	f5 SMALLINT,\n"
            + "	f6 INT,\n"
            + "	f7 BIGINT,\n"
            + "	f8 FLOAT,\n"
            + "	f9 DOUBLE,\n"
            + "	f10 DATE,\n"
            + "	f11 DATETIME,\n"
            + "	f12 TIMESTAMP);";

    public static final String OUTPUT_PART_TABLE = "dynamicSinkForPart";
    public static final String CREATE_ODPS_SINK_PART_TABLE =
            "CREATE TABLE "
                    + OUTPUT_PART_TABLE
                    + " (a int,b string,c string) partitioned by (d string,e string);";

    public static OdpsConf odpsConf;

    @BeforeClass
    public static void before() {
        odpsConf = OdpsTestUtils.getOdpsConf();
    }

    @After
    public void clearOutputTable() throws Exception {

    }

    @Test
    public void testBatchMode() throws Exception {
        TableEnvironment tEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        String ODPS_BATCH_SINK = "ODPS_BATCH_SINK";
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(ODPS_BATCH_SINK));
        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + ODPS_BATCH_SINK
                        + " ("
                        + "NAME STRING NOT NULL,"
                        + "SCORE BIGINT NOT NULL);");

        String ODPS_BATCH_SINK_PART = "ODPS_BATCH_SINK_PART";
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(ODPS_BATCH_SINK_PART));
        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + ODPS_BATCH_SINK_PART
                        + " ("
                        + "NAME STRING NOT NULL,"
                        + "SCORE BIGINT NOT NULL) partitioned by (dt string);");


        tEnv.executeSql(
                "CREATE TABLE USER_RESULT("
                        + "NAME STRING,"
                        + "SCORE BIGINT"
                        + ") WITH ( "
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
                        + ODPS_BATCH_SINK
                        + "',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        TableResult tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1, \"Bob\",1, \"Kim\",42, \"Kim\",42, \"Tom\",22]"
        );

        tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1, \"Bob\",1, \"Bob\",1, \"Bob\",1, \"Kim\",42, \"Kim\",42, \"Kim\",42, \"Kim\",42, \"Tom\",22, \"Tom\",22]"
        );

        tableResult =
                tEnv.executeSql(
                        "INSERT OVERWRITE USER_RESULT\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1, \"Bob\",1, \"Kim\",42, \"Kim\",42, \"Tom\",22]"
        );


        tEnv.executeSql(
                "CREATE TABLE USER_RESULT_PART("
                        + "NAME STRING,"
                        + "SCORE BIGINT,"
                        + "dt string"
                        + ") partitioned by (dt) WITH ( "
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
                        + ODPS_BATCH_SINK_PART
                        + "',"
                        + "'sink.max-retries' = '4'"
                        + ")");

        tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT_PART partition (dt='20200101')\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK_PART).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1,\"20200101\", \"Bob\",1,\"20200101\", \"Kim\",42,\"20200101\", \"Kim\",42,\"20200101\", \"Tom\",22,\"20200101\"]"
        );

        tableResult =
                tEnv.executeSql(
                        "INSERT INTO USER_RESULT_PART partition (dt='20200101')\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK_PART).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1,\"20200101\", \"Bob\",1,\"20200101\", \"Bob\",1,\"20200101\", \"Bob\",1,\"20200101\", \"Kim\",42,\"20200101\", \"Kim\",42,\"20200101\", \"Kim\",42,\"20200101\", \"Kim\",42,\"20200101\", \"Tom\",22,\"20200101\", \"Tom\",22,\"20200101\"]"
        );

        tableResult =
                tEnv.executeSql(
                        "INSERT overwrite USER_RESULT_PART partition (dt='20200101')\n"
                                + "SELECT user_name, score "
                                + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                + "(42, 'Kim'), (1, 'Bob')) "
                                + "AS UserCountTable(score, user_name)");
        tableResult.await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_BATCH_SINK_PART).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Bob\",1,\"20200101\", \"Bob\",1,\"20200101\", \"Kim\",42,\"20200101\", \"Kim\",42,\"20200101\", \"Tom\",22,\"20200101\"]"
        );

        try {
            tableResult =
                    tEnv.executeSql(
                            "INSERT overwrite USER_RESULT_PART \n"
                                    + "SELECT user_name, score, dt "
                                    + "FROM (VALUES (1, 'Bob', '20200101'), (22, 'Tom', '20200101'), (42, 'Kim', '20200101'), "
                                    + "(42, 'Kim', '20200101'), (1, 'Bob', '20200101')) "
                                    + "AS UserCountTable(score, user_name, dt)");
            tableResult.await();
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, "Cannot support dynamic partition in local mode by OdpsOutputFormat").isPresent());
        }

        try {
            tEnv.executeSql(
                    "CREATE TABLE USER_RESULT_PART_INVALID("
                            + "NAME STRING,"
                            + "SCORE BIGINT,"
                            + "dt string"
                            + ") partitioned by (dt) WITH ( "
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
                            + ODPS_BATCH_SINK
                            + "',"
                            + "'sink.max-retries' = '4'"
                            + ")");
            tableResult =
                    tEnv.executeSql(
                            "INSERT overwrite USER_RESULT_PART_INVALID partition (dt='20200101')\n"
                                    + "SELECT user_name, score "
                                    + "FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), "
                                    + "(42, 'Kim'), (1, 'Bob')) "
                                    + "AS UserCountTable(score, user_name)");
            tableResult.await();
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, "The partitionSpec should be null or whitespace with non partition odps table").isPresent());
        }
    }

    @Test
    public void testBatchModeWithDataGen() throws Exception {
        TableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInBatchMode();
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(OUTPUT_TABLE));
        OdpsTestUtils.exec(CREATE_ODPS_DATA_GEN_TABLE);
        tableEnv.executeSql(CREATE_DATAGEN_TABLE);
        tableEnv.executeSql(
                "CREATE TABLE odpsSink ("
                        + "	f1 STRING,\n"
                        + "	f2 BOOLEAN,\n"
                        + "	f3 DECIMAL(32,2),\n"
                        + "	f4 TINYINT,\n"
                        + "	f5 SMALLINT,\n"
                        + "	f6 INT,\n"
                        + "	f7 BIGINT,\n"
                        + "	f8 FLOAT,\n"
                        + "	f9 DOUBLE,\n"
                        + "	f10 DATE,\n"
                        + "	f11 TIMESTAMP(3),\n"
                        + "	f12 TIMESTAMP(9)\n"
                        + ") WITH ( "
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
                        + OUTPUT_TABLE
                        + "',"
                        + "'sink.max-retries' = '4',"
                        + "'sink.parallelism' = '10',"
                        + "'odps.cupid.writer.buffer.size' = '32mb'"
                        + ")");

        tableEnv.executeSql("INSERT INTO odpsSink SELECT * FROM datagen_t")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select count(*) from " + OUTPUT_TABLE).stream().sorted().collect(Collectors.toList()).toString(),
                "[100000]"
        );

        tableEnv.executeSql("INSERT INTO odpsSink SELECT * FROM datagen_t")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select count(*) from " + OUTPUT_TABLE).stream().sorted().collect(Collectors.toList()).toString(),
                "[200000]"
        );

        tableEnv.executeSql("INSERT OVERWRITE odpsSink SELECT * FROM datagen_t")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select count(*) from " + OUTPUT_TABLE).stream().sorted().collect(Collectors.toList()).toString(),
                "[100000]"
        );
    }

    @Test
    public void testStreamMode() throws Exception {
        String ODPS_STREAM_SINK = "odpsStreamSink";
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(ODPS_STREAM_SINK));
        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + ODPS_STREAM_SINK
                        + " ("
                        + "id INT NOT NULL,"
                        + "num BIGINT NOT NULL,"
                        + "ts TIMESTAMP);");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.enableCheckpointing(1000);
        StreamTableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        Table t =
                tableEnv.fromDataStream(
                        get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"));

        tableEnv.registerTable("T", t);
        tableEnv.executeSql(
                "CREATE TABLE odpsSink ("
                        + "  id INT,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(9)"
                        + ") WITH ( "
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
                        + ODPS_STREAM_SINK
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '2',"
                        + "'sink.buffer-flush.interval' = '120s',"
                        + "'sink.max-retries' = '4'"
                        + ")");


        tableEnv.executeSql("INSERT INTO odpsSink SELECT id, num, ts FROM T WHERE id IN (2, 10, 20)")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_STREAM_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[10,4,\"1970-01-01 00:00:00.01\", 2,2,\"1970-01-01 00:00:00.002\", 20,6,\"1970-01-01 00:00:00.02\"]"
        );


        tableEnv.executeSql("INSERT INTO odpsSink SELECT id, num, ts FROM T WHERE id IN (1, 5, 9)")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_STREAM_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[1,1,\"1970-01-01 00:00:00.001\", 10,4,\"1970-01-01 00:00:00.01\", 2,2,\"1970-01-01 00:00:00.002\", 20,6,\"1970-01-01 00:00:00.02\", 5,3,\"1970-01-01 00:00:00.005\", 9,4,\"1970-01-01 00:00:00.009\"]"
        );

        try {
            tableEnv.executeSql("INSERT OVERWRITE odpsSink SELECT id, num, ts FROM T WHERE id IN (1, 5, 9)")
                    .await();
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, "Streaming mode not support overwrite").isPresent());
        }
    }

    @Test
    public void testStreamModePartTable() throws Exception {
        String ODPS_STREAM_PART_SINK = "odpsStreamPartSink";
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(ODPS_STREAM_PART_SINK));
        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + ODPS_STREAM_PART_SINK
                        + " ("
                        + "text STRING,"
                        + "num BIGINT NOT NULL,"
                        + "ts TIMESTAMP) partitioned by (id int);");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.enableCheckpointing(1000);
        StreamTableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        Table t =
                tableEnv.fromDataStream(
                        get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"), $("user_action_time").proctime());

        tableEnv.registerTable("T", t);

        tableEnv.executeSql(
                "CREATE TABLE odpsPartSink ("
                        + "  text string,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(9),"
                        + "  id INT"
                        + ") partitioned by (id) WITH ( "
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
                        + ODPS_STREAM_PART_SINK
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '10',"
                        + "'sink.buffer-flush.interval' = '120s',"
                        + "'sink.max-retries' = '4'"
                        + ")");
        tableEnv.executeSql("INSERT INTO odpsPartSink partition (id = 1) SELECT text, num, ts FROM T WHERE num=5")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select * from " + ODPS_STREAM_PART_SINK).stream().sorted().collect(Collectors.toList()).toString(),
                "[\"Comment#5\",5,\"1970-01-01 00:00:00.011\",1, \"Comment#6\",5,\"1970-01-01 00:00:00.012\",1, \"Comment#7\",5,\"1970-01-01 00:00:00.013\",1, \"Comment#8\",5,\"1970-01-01 00:00:00.014\",1, \"Comment#9\",5,\"1970-01-01 00:00:00.015\",1]"
        );
    }

    @Test
    public void testDynamicLimit() throws Exception {
        String ODPS_STREAM_PART_SINK = "odpsStreamPartSink";
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(ODPS_STREAM_PART_SINK));
        OdpsTestUtils.exec(
                "CREATE TABLE "
                        + ODPS_STREAM_PART_SINK
                        + " ("
                        + "text STRING,"
                        + "num BIGINT NOT NULL,"
                        + "ts TIMESTAMP) partitioned by (id int);");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        Table t =
                tableEnv.fromDataStream(
                        get4TupleDataStream(env), $("id"), $("num"), $("text"), $("ts"), $("user_action_time").proctime());

        tableEnv.registerTable("T", t);

        tableEnv.executeSql(
                "CREATE TABLE odpsPartSink ("
                        + "  text string,"
                        + "  num BIGINT,"
                        + "  ts TIMESTAMP(9),"
                        + "  id INT"
                        + ") partitioned by (id) WITH ( "
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
                        + ODPS_STREAM_PART_SINK
                        + "',"
                        + "'sink.buffer-flush.max-rows' = '0',"
                        + "'sink.buffer-flush.max-size' = '0',"
                        + "'sink.buffer-flush.interval' = '0',"
                        + "'sink.dynamic-partition.limit' = '25',"
                        + "'sink.max-retries' = '4'"
                        + ")");
        tableEnv.executeSql("INSERT INTO odpsPartSink SELECT text, num, ts,id  FROM T")
                .await();
    }

    @Test
    public void testStreamModeWithDataGen() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.enableCheckpointing(1000);

        StreamTableEnvironment tableEnv = OdpsCatalogUtils.createTableEnvInStreamingMode(env);
        OdpsTestUtils.exec(OdpsTestUtils.dropOdpsTable(OUTPUT_TABLE));
        OdpsTestUtils.exec(CREATE_ODPS_DATA_GEN_TABLE);
        tableEnv.executeSql(CREATE_DATAGEN_TABLE);
        tableEnv.executeSql(
                "CREATE TABLE odpsSink ("
                        + "	f1 STRING,\n"
                        + "	f2 BOOLEAN,\n"
                        + "	f3 DECIMAL(32,2),\n"
                        + "	f4 TINYINT,\n"
                        + "	f5 SMALLINT,\n"
                        + "	f6 INT,\n"
                        + "	f7 BIGINT,\n"
                        + "	f8 FLOAT,\n"
                        + "	f9 DOUBLE,\n"
                        + "	f10 DATE,\n"
                        + "	f11 TIMESTAMP(3),\n"
                        + "	f12 TIMESTAMP(9)\n"
                        + ") WITH ( "
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
                        + OUTPUT_TABLE
                        + "',"
                        + "'sink.max-retries' = '4',"
                        + "'sink.parallelism' = '10'"
                        + ")");

        tableEnv.executeSql("INSERT INTO odpsSink SELECT * FROM datagen_t")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select count(*) from " + OUTPUT_TABLE).stream().sorted().collect(Collectors.toList()).toString(),
                "[100000]"
        );

        tableEnv.executeSql("INSERT INTO odpsSink SELECT * FROM datagen_t")
                .await();
        Assert.assertEquals(
                QUERY_BY_ODPS_SQL("select count(*) from " + OUTPUT_TABLE).stream().sorted().collect(Collectors.toList()).toString(),
                "[200000]"
        );

        try {
            tableEnv.executeSql("INSERT OVERWRITE odpsSink SELECT * FROM datagen_t")
                    .await();
        } catch (Exception e) {
            assertTrue(findThrowableWithMessage(e, "Streaming mode not support overwrite").isPresent());
        }
    }


    public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(
            StreamExecutionEnvironment env) {
        List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
        data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(
                new Tuple4<>(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(
                new Tuple4<>(
                        6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
        data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
        data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
        data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
        data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
        data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
        data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
        data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
        data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
        data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
        data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
        data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
        data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
        data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));
        Collections.shuffle(data);
        return env.fromCollection(data);
    }

}
