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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.util.DateString;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableRecordBuilderTest {

    private DsConfiguration dsConfiguration = new DataSourceConfig();
    private Configure configure = new Configure();
    private ExecutorService executor = Executors.newFixedThreadPool(1);

    @Test
    public void testAddRecord() {
        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));
        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnAfterValue("testNormal"));
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation operation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("123456"), 123456, 123456, dsRecord);

        Op op = new Op(operation, tableMetaData, dsConfiguration);

        TableMapping tableMapping = new TableMapping();
        configure.setBuildRecordQueueSize(1);
        TableRecordBuilder builder = new TableRecordBuilder(configure, tableMapping, executor);
        builder.start();

        boolean ret = builder.addRecord(op, "I", "123");
        Assert.assertTrue(ret);

        ret = builder.addRecord(op, "I", "123");
        Assert.assertFalse(ret);

        builder.stop();
        try {
            builder.addRecord(op, "I", "123");
            Assert.fail();
        } catch (RuntimeException ignored) {
        }
    }

    @Test
    public void testBuildTupleRecordNormal() throws Exception {
        TableMapping tableMapping = new TableMapping();
        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("bigint_field_old", FieldType.BIGINT));
            addField(new Field("timestamp_field", FieldType.TIMESTAMP));
            addField(new Field("timestamp_field_old", FieldType.TIMESTAMP));
            addField(new Field("string_field", FieldType.STRING));
            addField(new Field("string_field_old", FieldType.STRING));
            addField(new Field("double_field", FieldType.DOUBLE));
            addField(new Field("double_field_old", FieldType.DOUBLE));
            addField(new Field("boolean_field", FieldType.BOOLEAN));
            addField(new Field("boolean_field_old", FieldType.BOOLEAN));
            addField(new Field("decimal_field", FieldType.DECIMAL));
            addField(new Field("decimal_field_old", FieldType.DECIMAL));
            addField(new Field("integer_field", FieldType.INTEGER));
            addField(new Field("integer_field_old", FieldType.INTEGER));
            addField(new Field("float_field", FieldType.FLOAT));
            addField(new Field("float_field_old", FieldType.FLOAT));
            addField(new Field("tinyint_field", FieldType.TINYINT));
            addField(new Field("tinyint_field_old", FieldType.TINYINT));
            addField(new Field("smallint_field", FieldType.SMALLINT));
            addField(new Field("smallint_field_old", FieldType.SMALLINT));
        }};

        tableMapping.setRecordSchema(recordSchema);
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("bigint_field").setDestOld("bigint_field_old"));
        columns.put("c2", new ColumnMapping().setSrc("c2").setDest("timestamp_field").setDestOld("timestamp_field_old"));
        columns.put("c3", new ColumnMapping().setSrc("c3").setDest("string_field").setDestOld("string_field_old"));
        columns.put("c4", new ColumnMapping().setSrc("c4").setDest("double_field").setDestOld("double_field_old"));
        columns.put("c5", new ColumnMapping().setSrc("c5").setDest("boolean_field").setDestOld("boolean_field_old"));
        columns.put("c6", new ColumnMapping().setSrc("c6").setDest("decimal_field").setDestOld("decimal_field_old"));
        columns.put("c7", new ColumnMapping().setSrc("c7").setDest("integer_field").setDestOld("integer_field_old"));
        columns.put("c8", new ColumnMapping().setSrc("c8").setDest("float_field").setDestOld("float_field_old"));
        columns.put("c9", new ColumnMapping().setSrc("c9").setDest("tinyint_field").setDestOld("tinyint_field_old"));
        columns.put("c10", new ColumnMapping().setSrc("c10").setDest("smallint_field").setDestOld("smallint_field_old"));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildTupleRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));
        columnMetaDataList.add(new ColumnMetaData("c2", 1));
        columnMetaDataList.add(new ColumnMetaData("c3", 2));
        columnMetaDataList.add(new ColumnMetaData("c4", 3));
        columnMetaDataList.add(new ColumnMetaData("c5", 4));
        columnMetaDataList.add(new ColumnMetaData("c6", 5));
        columnMetaDataList.add(new ColumnMetaData("c7", 6));
        columnMetaDataList.add(new ColumnMetaData("c8", 7));
        columnMetaDataList.add(new ColumnMetaData("c9", 8));
        columnMetaDataList.add(new ColumnMetaData("c10", 9));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("1"), new DsColumnBeforeValue("11")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("2020-11-11 11:11:11"), new DsColumnBeforeValue("2019-11-11 11:11:11")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("after"), new DsColumnBeforeValue("before")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("3.1234"), new DsColumnBeforeValue("4.1234")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("true"), new DsColumnBeforeValue("false")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("5.1234"), new DsColumnBeforeValue("6.1234")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("2"), new DsColumnBeforeValue("22")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("7.1234"), new DsColumnBeforeValue("8.1234")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("3"), new DsColumnBeforeValue("33")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("4"), new DsColumnBeforeValue("44")));

        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_UPDATE, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("U", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();

        Assert.assertEquals(data.getRecordSchema().getFields().size(), 20);
        Assert.assertEquals(data.getField("bigint_field"), 1L);
        Assert.assertEquals(data.getField("bigint_field_old"), 11L);
        Assert.assertEquals(data.getField("timestamp_field"), 1605064271000000L);
        Assert.assertEquals(data.getField("timestamp_field_old"), 1573441871000000L);
        Assert.assertEquals(data.getField("string_field"), "after");
        Assert.assertEquals(data.getField("string_field_old"), "before");
        Assert.assertEquals(data.getField("double_field"), 3.1234);
        Assert.assertEquals(data.getField("double_field_old"), 4.1234);
        Assert.assertEquals(data.getField("boolean_field"), true);
        Assert.assertEquals(data.getField("boolean_field_old"), false);
        Assert.assertEquals(data.getField("decimal_field"), new BigDecimal("5.1234"));
        Assert.assertEquals(data.getField("decimal_field_old"), new BigDecimal("6.1234"));
        Assert.assertEquals(data.getField("integer_field"), 2);
        Assert.assertEquals(data.getField("integer_field_old"), 22);
        Assert.assertEquals(data.getField("float_field"), 7.1234f);
        Assert.assertEquals(data.getField("float_field_old"), 8.1234f);
        Assert.assertEquals(data.getField("tinyint_field"), (byte) 3);
        Assert.assertEquals(data.getField("tinyint_field_old"), (byte) 33);
        Assert.assertEquals(data.getField("smallint_field"), (short) 4);
        Assert.assertEquals(data.getField("smallint_field_old"), (short) 44);
    }

    @Test
    public void testBuildTupleWithExtraFiled() throws Exception {
        TableMapping tableMapping = new TableMapping();
        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("cid", FieldType.STRING));
            addField(new Field("ctype", FieldType.STRING));
            addField(new Field("ctime", FieldType.STRING));
        }};

        tableMapping.setRecordSchema(recordSchema);
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");
        tableMapping.setcIdColumn("cid");
        tableMapping.setcTypeColumn("ctype");
        tableMapping.setcTimeColumn("ctime");

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("bigint_field"));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildTupleRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("1")));

        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("I", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();

        Assert.assertEquals(data.getRecordSchema().getFields().size(), 4);
        Assert.assertEquals(data.getField("bigint_field"), 1L);
        Assert.assertEquals(data.getField("cid"), "test_id");
        Assert.assertEquals(data.getField("ctype"), "I");
        Assert.assertEquals(data.getField("ctime"), "2020-11-11 11:11:11");
    }

    @Test
    public void testBuildTupleWithConstColumn() throws Exception {
        TableMapping tableMapping = new TableMapping();
        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("ctime", FieldType.TIMESTAMP));
            addField(new Field("f1", FieldType.STRING));
            addField(new Field("f2", FieldType.STRING));
            addField(new Field("f3", FieldType.STRING));
        }};

        tableMapping.setRecordSchema(recordSchema);
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");
        tableMapping.setcTimeColumn("ctime");
        Map<String, String> constColumns = new HashMap<>();
        constColumns.put("f1", "test");
        constColumns.put("f2", "%Y%m%d");
        constColumns.put("f3", "%D");
        tableMapping.setConstColumnMappings(constColumns);

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("bigint_field"));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildTupleRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("1")));

        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("I", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();

        Assert.assertEquals(data.getRecordSchema().getFields().size(), 5);
        Assert.assertEquals(data.getField("bigint_field"), 1L);
        Assert.assertEquals(data.getField("ctime"), 1605064271000000L);
        Assert.assertEquals(data.getField("f1"), "test");
        Assert.assertEquals(data.getField("f2"), "20201111");
        Assert.assertEquals(data.getField("f3"), "11/11/20");
    }

    @Test
    public void testBuildTupleWithKeyColumn() throws Exception {
        TableMapping tableMapping = new TableMapping();
        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
        }};

        tableMapping.setRecordSchema(recordSchema);
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("bigint_field").setIsKeyColumn(true));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildTupleRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnBeforeValue("22"));


        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_UPDATE, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("U", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();

        Assert.assertEquals(data.getRecordSchema().getFields().size(), 1);
        Assert.assertEquals(data.getField("bigint_field"), 22L);
    }

    @Test
    public void testBuildTupleWithHashKey() throws Exception {
        TableMapping tableMapping = new TableMapping();
        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("string_field", FieldType.STRING));
        }};

        tableMapping.setRecordSchema(recordSchema);
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("bigint_field").setIsShardColumn(true));
        columns.put("c2", new ColumnMapping().setSrc("c2").setDest("string_field").setIsShardColumn(true));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildTupleRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));
        columnMetaDataList.add(new ColumnMetaData("c2", 1));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnAfterValue("11"));
        dsColumnList.add(new DsColumnAfterValue("test"));


        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("I", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();

        Assert.assertEquals(recordEntry.getPartitionKey(), "11test");
        Assert.assertEquals(data.getRecordSchema().getFields().size(), 2);
        Assert.assertEquals(data.getField("bigint_field"), 11L);
        Assert.assertEquals(data.getField("string_field"), "test");
    }

    @Test
    public void testBuildBlobNormal() throws Exception {
        TableMapping tableMapping = new TableMapping();
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");
        Map<String, String> constColumns = new HashMap<>();
        tableMapping.setConstColumnMappings(constColumns);

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("field"));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildBlobRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("test")));

        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("I", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();
        method.invoke(instance, record, tableMapping, recordEntry);
        BlobRecordData data = (BlobRecordData) recordEntry.getRecordData();

        Assert.assertEquals(new String(data.getData()), "test");
    }

    @Test
    public void testBuildBlobNormalWithMultiColumn() throws Exception {
        TableMapping tableMapping = new TableMapping();
        tableMapping.setOracleTableName("test_table");
        tableMapping.setOracleFullTableName("test_schema.test_table");
        Map<String, String> constColumns = new HashMap<>();
        tableMapping.setConstColumnMappings(constColumns);

        Map<String, ColumnMapping> columns = new HashMap<>();
        columns.put("c1", new ColumnMapping().setSrc("c1").setDest("field1"));
        columns.put("c2", new ColumnMapping().setSrc("c2").setDest("field2"));
        tableMapping.setColumnMappings(columns);

        Class<TableRecordBuilder> builderClass = TableRecordBuilder.class;
        Object instance = builderClass.getDeclaredConstructor(new Class[]{Configure.class, TableMapping.class, ExecutorService.class}).newInstance(configure, tableMapping, executor);
        Method method = builderClass.getDeclaredMethod("buildBlobRecord", TableRecordBuilder.Record.class, TableMapping.class, RecordEntry.class);
        //值为true时 反射的对象在使用时 让一切已有的访问权限取消
        method.setAccessible(true);

        ArrayList<ColumnMetaData> columnMetaDataList = new ArrayList<>();
        columnMetaDataList.add(new ColumnMetaData("c1", 0));

        ArrayList<DsColumn> dsColumnList = new ArrayList<>();
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("test")));
        dsColumnList.add(new DsColumnComposite(new DsColumnAfterValue("test")));

        TableMetaData tableMetaData = new TableMetaData(new TableName("test_table"), columnMetaDataList);
        DsRecord dsRecord = new DsRecord(dsColumnList);
        DsOperation dsOperation = new DsOperation(new TableName("test_table"), tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2020-11-11 11:11:11"), 123456, 123456, dsRecord);
        Op op = new Op(dsOperation, tableMetaData, dsConfiguration);

        TableRecordBuilder.Record record = new TableRecordBuilder.Record("I", "test_id", op);
        RecordEntry recordEntry = new RecordEntry();

        try {
            method.invoke(instance, record, tableMapping, recordEntry);
            Assert.fail();
        } catch (InvocationTargetException ignored) {
        }
    }
}
