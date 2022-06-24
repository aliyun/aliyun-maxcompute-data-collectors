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

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.exception.MalformedRecordException;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.PutRecordsByShardResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ShardWriterTest {
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private Configure configure;
    private DatahubClient client;

    @BeforeMethod
    public void setup() {
        client = mock(DatahubClient.class);
        ClientHelper.initForTest(client);

        configure = new Configure();
        configure.setDirtyDataFile("test.file");
    }

    @AfterMethod
    public void teardown() throws IOException {
        File file = new File(configure.getDirtyDataFile());
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
    }

    @Test
    public void testWriteRecordNormal() {
        ShardWriter writer = new ShardWriter(configure, executor, "test_table", "test_project", "test_topic", "0");
        writer.start();

        PutRecordsByShardResult putRecordsByShardResult = new PutRecordsByShardResult();
        doReturn(putRecordsByShardResult).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());

        BlobRecordData data1 = new BlobRecordData("test1".getBytes());
        BlobRecordData data2 = new BlobRecordData("test2".getBytes());
        RecordEntry recordEntry1 = new RecordEntry();
        recordEntry1.setRecordData(data1);
        RecordEntry recordEntry2 = new RecordEntry();
        recordEntry2.setRecordData(data2);
        writer.writeRecord(recordEntry1);
        writer.writeRecord(recordEntry2);

        writer.syncExec();
        writer.close();

        ArgumentCaptor<List<RecordEntry>> argument = ArgumentCaptor.forClass(List.class);
        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), argument.capture());

        List<RecordEntry> recordEntryList = argument.getValue();
        Assert.assertEquals(recordEntryList.size(), 2);
        BlobRecordData record1 = (BlobRecordData) recordEntryList.get(0).getRecordData();
        BlobRecordData record2 = (BlobRecordData) recordEntryList.get(1).getRecordData();
        Assert.assertEquals(record1.getData(), "test1".getBytes());
        Assert.assertEquals(record2.getData(), "test2".getBytes());
    }

    @Test
    public void testDirtyDataCanRetry() throws IOException {
        configure.setDirtyDataContinue(true);
        configure.setRetryTimes(1);
        ShardWriter writer = new ShardWriter(configure, executor, "test_table", "test_project", "test_topic", "0");
        writer.start();

        MalformedRecordException exception = new MalformedRecordException("Record field size not match");
        doThrow(exception).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());

        BlobRecordData data1 = new BlobRecordData("test1".getBytes());
        RecordEntry recordEntry1 = new RecordEntry();
        recordEntry1.setRecordData(data1);
        writer.writeRecord(recordEntry1);

        writer.syncExec();
        writer.close();

        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());

        String jsonStr = FileUtils.readFileToString(new File(configure.getDirtyDataFile()));
        JsonNode jsonNode= JsonHelper.getJsonNodeFromString(jsonStr);
        Assert.assertNotNull(jsonNode);
        Assert.assertEquals(jsonNode.get("oracleTable").asText(), "test_table");
        Assert.assertEquals(jsonNode.get("topicName").asText(), "test_topic");
        Assert.assertEquals(jsonNode.get("errorMessage").asText(), "Record field size not match");
        Assert.assertEquals(jsonNode.get("record").get("content").asText(), "test1");
    }

    @Test
    public void testDirtyDataNotContinue() {
        configure.setDirtyDataContinue(false);
        ShardWriter writer = new ShardWriter(configure, executor, "test_table", "test_project", "test_topic", "0");
        writer.start();

        MalformedRecordException exception = new MalformedRecordException("Record field size not match");
        doThrow(exception).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());

        BlobRecordData data1 = new BlobRecordData("test1".getBytes());
        RecordEntry recordEntry1 = new RecordEntry();
        recordEntry1.setRecordData(data1);
        writer.writeRecord(recordEntry1);

        try {
            writer.syncExec();
            Assert.fail();
        } catch (RuntimeException e) {
            Assert.assertEquals(e.getMessage(), "[httpStatus:5001, requestId:null, errorCode:null, errorMessage:Record field size not match]");
        }

        writer.close();
        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());
    }
}
