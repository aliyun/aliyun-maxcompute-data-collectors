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

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import com.fasterxml.jackson.databind.JsonNode;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsColumnAfterValue;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsRecord;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.util.DateString;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.math.BigDecimal;
import java.util.ArrayList;

/**
 * Created by lyf0429 on 16/5/25.
 */
public class BadOperateWriterTest {

    private String fileName = "test.txt";

    @BeforeMethod
    public void setUpBeforeMethod() {

    }

    @AfterMethod
    public void tearDownAfterMethod() throws IOException {
        File file = new File(fileName);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }

        file = new File(fileName + ".bak");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
    }

    @Test
    public void testCheckFileSize() throws IOException {
        for (int i = 0; i < 5; i++) {
            String content = "hello,world";

            if (i == 1) {
                content = "hello,liangyf";
            }

            File file = new File(fileName);
            FileUtils.writeStringToFile(file, content);
            BadOperateWriter.checkFileSize(fileName, 1);

            Assert.assertFalse(file.exists());

            String fileContent = FileUtils.readFileToString(new File(fileName + ".bak"));
            Assert.assertEquals(fileContent, content);
        }
    }

    @Test
    public void testWriteOp() throws IOException {
        ArrayList<ColumnMetaData> columnMetaDatas = new ArrayList<ColumnMetaData>();

        ColumnMetaData columnMetaData = new ColumnMetaData("c1", 1);
        columnMetaDatas.add(columnMetaData);
        columnMetaData = new ColumnMetaData("c2", 2);
        columnMetaDatas.add(columnMetaData);
        columnMetaData = new ColumnMetaData("c3", 3);
        columnMetaDatas.add(columnMetaData);
        columnMetaData = new ColumnMetaData("c4", 4);
        columnMetaDatas.add(columnMetaData);
        columnMetaData = new ColumnMetaData("c5", 5);
        columnMetaDatas.add(columnMetaData);


        TableName tableName = new TableName("ogg_test.t_person");

        TableMetaData tableMetaData = new TableMetaData(tableName, columnMetaDatas);

        DsMetaData dsMetaData = new DsMetaData();
        dsMetaData.setTableMetaData(tableMetaData);
        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("6");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, tableMetaData, DsOperation.OpType.DO_INSERT, new DateString("2016-05-13 19:15:15.010"), 0L, 0L, dsRecord);

        Op op = new Op(dsOperation, tableMetaData, null);

        BadOperateWriter.write(op,
                "ogg_test.t_person",
                "t_person",
                fileName,
                10000,
                "op error");

        //FileUtils.deleteDirectory(new File("op.txt"));
        String jsonStr = FileUtils.readFileToString(new File(fileName));
        JsonNode jsonNode = JsonHelper.getJsonNodeFromString(jsonStr);
        Assert.assertNotNull(jsonNode);

        Assert.assertEquals(jsonNode.get("oracleTable").asText(), "ogg_test.t_person");
        Assert.assertEquals(jsonNode.get("topicName").asText(), "t_person");
        Assert.assertEquals(jsonNode.get("errorMessage").asText(), "op error");

        JsonNode jsonRecord = jsonNode.get("record");
        Assert.assertNotNull(jsonRecord);
        Assert.assertEquals(jsonRecord.get("c1").asText(), "testNormal");
        Assert.assertEquals(jsonRecord.get("c2").asText(), "2");
        Assert.assertEquals(jsonRecord.get("c3").asText(), "3");
        Assert.assertEquals(jsonRecord.get("c4").asText(), "2016-05-20 09:00:00");
        Assert.assertEquals(jsonRecord.get("c5").asText(), "6");
    }

    @Test
    public void testWriteRecord() throws IOException {

        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("timestamp_field", FieldType.TIMESTAMP));
            addField(new Field("string_field", FieldType.STRING));
            addField(new Field("double_field", FieldType.DOUBLE));
            addField(new Field("boolean_field", FieldType.BOOLEAN));
            addField(new Field("decimal_field", FieldType.DECIMAL));
            addField(new Field("integer_field", FieldType.INTEGER));
            addField(new Field("float_field", FieldType.FLOAT));
            addField(new Field("tinyint_field", FieldType.TINYINT));
            addField(new Field("smallint_field", FieldType.SMALLINT));
        }};

        RecordEntry recordEntry = new RecordEntry();
        TupleRecordData recordData = new TupleRecordData(recordSchema);
        long timestamp = System.currentTimeMillis();

        recordData.setField("bigint_field", timestamp + 2);
        recordData.setField("timestamp_field", timestamp);
        recordData.setField("string_field", "abcdefg");
        recordData.setField("double_field", 3.1415926);
        recordData.setField("boolean_field", true);
        recordData.setField("decimal_field", new BigDecimal("3.1415926"));
        recordData.setField("integer_field", 1);
        recordData.setField("float_field", 3.15);
        recordData.setField("tinyint_field", 2);
        recordData.setField("smallint_field", 3);

        recordEntry.setShardId("1");
        recordEntry.setRecordData(recordData);


        BadOperateWriter.write(recordEntry,
                "ogg_test.t_person",
                "t_person",
                fileName,
                10000,
                "record error");

        String jsonStr = FileUtils.readFileToString(new File(fileName));
        JsonNode jsonNode = JsonHelper.getJsonNodeFromString(jsonStr);
        Assert.assertNotNull(jsonNode);

        Assert.assertEquals(jsonNode.get("oracleTable").asText(), "ogg_test.t_person");
        Assert.assertEquals(jsonNode.get("topicName").asText(), "t_person");
        Assert.assertEquals(jsonNode.get("errorMessage").asText(), "record error");

        JsonNode jsonRecord = jsonNode.get("record");
        Assert.assertNotNull(jsonRecord);
        Assert.assertEquals(jsonRecord.get("bigint_field").asLong(), timestamp + 2);
        Assert.assertEquals(jsonRecord.get("timestamp_field").asLong(), timestamp);
        Assert.assertEquals(jsonRecord.get("string_field").asText(), "abcdefg");
        Assert.assertEquals(jsonRecord.get("double_field").asDouble(), 3.1415926);
        Assert.assertTrue(jsonRecord.get("boolean_field").asBoolean());
        Assert.assertEquals(jsonRecord.get("decimal_field").asText(), "3.1415926");
        Assert.assertEquals(jsonRecord.get("integer_field").asInt(), 1);
        Assert.assertEquals((float) jsonRecord.get("float_field").asDouble(), 3.15f);
        Assert.assertEquals((byte) jsonRecord.get("tinyint_field").asInt(), 2);
        Assert.assertEquals((short) jsonRecord.get("smallint_field").asInt(), 3);
    }
}
