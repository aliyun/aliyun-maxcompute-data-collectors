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


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.client.model.*;
import oracle.goldengate.datasource.DsColumn;
import oracle.goldengate.datasource.DsColumnAfterValue;
import oracle.goldengate.datasource.DsOperation;
import oracle.goldengate.datasource.DsRecord;
import oracle.goldengate.datasource.adapt.Op;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
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
        if (file.exists()){
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

    @Test(testName = "测试写入op")
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

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0l, 0l, dsRecord);

        Op op = new Op(dsOperation, tableMetaData, null);

        BadOperateWriter.write(op,
                "ogg_test.t_person",
                "t_person",
                fileName,
                10000,
                "op error");

        //FileUtils.deleteDirectory(new File("op.txt"));
        String jsonStr = FileUtils.readFileToString(new File(fileName));
        JSONObject object = JSON.parseObject(jsonStr);
        Assert.assertEquals(object.getString("oracleTable"), "ogg_test.t_person");
        Assert.assertEquals(object.getString("topicName"), "t_person");
        Assert.assertNull(object.getString("shardId"));
        Assert.assertEquals(object.getString("errorMessage"), "op error");

        JSONObject record = object.getJSONObject("record");
        Assert.assertEquals(record.getString("c1"), "testNormal");
        Assert.assertEquals(record.getString("c2"), "2");
        Assert.assertEquals(record.getString("c3"), "3");
        Assert.assertEquals(record.getString("c4"), "2016-05-20 09:00:00");
        Assert.assertEquals(record.getString("c5"), "6");
    }

    @Test(testName = "测试写入record")
    public void testWriteRecord() throws IOException {

        RecordSchema recordSchema = new RecordSchema() {{
            addField(new Field("bigint_field", FieldType.BIGINT));
            addField(new Field("timestamp_field", FieldType.TIMESTAMP));
            addField(new Field("string_field", FieldType.STRING));
            addField(new Field("double_field", FieldType.DOUBLE));
            addField(new Field("boolean_field", FieldType.BOOLEAN));
            addField(new Field("decimal_field", FieldType.DECIMAL));
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

        recordEntry.setShardId("1");
        recordEntry.setRecordData(recordData);


        BadOperateWriter.write(recordEntry,
                "ogg_test.t_person",
                "t_person",
                fileName,
                10000,
                "record error");

        String jsonStr = FileUtils.readFileToString(new File(fileName));
        JSONObject object = JSON.parseObject(jsonStr);
        Assert.assertEquals(object.getString("oracleTable"), "ogg_test.t_person");
        Assert.assertEquals(object.getString("topicName"), "t_person");
        Assert.assertNull(object.getString("shardId"), null);
        Assert.assertEquals(object.getString("errorMessage"), "record error");

        JSONObject record = object.getJSONObject("record");
        Assert.assertEquals(record.getLongValue("bigint_field"), timestamp + 2);
        Assert.assertEquals(record.getLongValue("timestamp_field"), timestamp);
        Assert.assertEquals(record.getString("string_field"), "abcdefg");
        Assert.assertEquals(record.getDoubleValue("double_field"), 3.1415926);
        Assert.assertTrue(record.getBooleanValue("boolean_field"));
        Assert.assertEquals(record.getBigDecimal("decimal_field"), new BigDecimal("3.1415926"));
    }
}
