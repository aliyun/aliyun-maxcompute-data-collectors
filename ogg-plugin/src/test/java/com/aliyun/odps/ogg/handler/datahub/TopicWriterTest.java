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
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.exception.ShardSealedException;
import com.aliyun.datahub.client.model.BlobRecordData;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.PutRecordsByShardResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import org.apache.commons.io.FileUtils;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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
import static org.mockito.Mockito.when;

public class TopicWriterTest {
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    private Configure configure;
    private DatahubClient client;

    @BeforeMethod
    public void setup() {
        client = mock(DatahubClient.class);
        ClientHelper.initForTest(client);

        configure = new Configure();
//        Map<String, TableMapping> tableMappingMaps = new HashMap<>();
//        tableMappingMaps.put("test_table", new TableMapping().setProjectName("test_project").setTopicName("test_topic"));
//        configure.setTableMappings(tableMappingMaps);
//        configure.setDirtyDataFile("test.file");
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
        TableMapping tableMapping = new TableMapping();
        tableMapping.setProjectName("test_project").setTopicName("test_topic");
        TopicWriter writer = new TopicWriter(configure, tableMapping, executor);

        ShardEntry entry = new ShardEntry();
        entry.setShardId("0");
        entry.setState(ShardState.ACTIVE);
        List<ShardEntry> shardEntryList = Collections.singletonList(entry);
        ListShardResult listShardResult = new ListShardResult();
        listShardResult.setShards(shardEntryList);
        doReturn(listShardResult).when(client).listShard(eq("test_project"), eq("test_topic"));

        PutRecordsByShardResult putRecordsByShardResult = new PutRecordsByShardResult();
        doReturn(putRecordsByShardResult).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());

        writer.start();

        BlobRecordData data1 = new BlobRecordData("test1".getBytes());
        BlobRecordData data2 = new BlobRecordData("test2".getBytes());
        RecordEntry recordEntry1 = new RecordEntry();
        recordEntry1.setRecordData(data1);
        RecordEntry recordEntry2 = new RecordEntry();
        recordEntry2.setRecordData(data2);
        writer.writeRecord(recordEntry1);
        writer.writeRecord(recordEntry2);

        writer.syncExec();
        writer.stop();

        verify(client, times(1)).listShard(eq("test_project"), eq("test_topic"));
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
    public void testWriteRecordWithHash() {
        TableMapping tableMapping = new TableMapping();
        tableMapping.setProjectName("test_project").setTopicName("test_topic");
        TopicWriter writer = new TopicWriter(configure, tableMapping, executor);

        ShardEntry entry1 = new ShardEntry();
        entry1.setShardId("0");
        entry1.setState(ShardState.ACTIVE);
        ShardEntry entry2 = new ShardEntry();
        entry2.setShardId("1");
        entry2.setState(ShardState.ACTIVE);
        List<ShardEntry> shardEntryList = Arrays.asList(entry1, entry2);
        ListShardResult listShardResult = new ListShardResult();
        listShardResult.setShards(shardEntryList);
        doReturn(listShardResult).when(client).listShard(eq("test_project"), eq("test_topic"));

        PutRecordsByShardResult putRecordsByShardResult = new PutRecordsByShardResult();
        doReturn(putRecordsByShardResult).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());
        doReturn(putRecordsByShardResult).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("1"), any());

        writer.start();

        BlobRecordData data1 = new BlobRecordData("test1".getBytes());
        BlobRecordData data2 = new BlobRecordData("test2".getBytes());
        RecordEntry recordEntry1 = new RecordEntry();
        recordEntry1.setRecordData(data1);
        recordEntry1.setPartitionKey("aaaaa"); // hash code为92567585，写入1shard
        RecordEntry recordEntry2 = new RecordEntry();
        recordEntry2.setRecordData(data2);
        recordEntry2.setPartitionKey("bbbbb"); // hash code为93521890，写入0shard
        writer.writeRecord(recordEntry1);
        writer.writeRecord(recordEntry2);

        writer.syncExec();
        writer.stop();

        verify(client, times(1)).listShard(eq("test_project"), eq("test_topic"));
        ArgumentCaptor<List<RecordEntry>> argument = ArgumentCaptor.forClass(List.class);
        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), argument.capture());

        List<RecordEntry> recordEntryList = argument.getValue();
        Assert.assertEquals(recordEntryList.size(), 1);
        BlobRecordData record1 = (BlobRecordData) recordEntryList.get(0).getRecordData();
        Assert.assertEquals(record1.getData(), "test2".getBytes());
        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("1"), argument.capture());
        recordEntryList = argument.getValue();
        Assert.assertEquals(recordEntryList.size(), 1);
        BlobRecordData record2 = (BlobRecordData) recordEntryList.get(0).getRecordData();
        Assert.assertEquals(record2.getData(), "test1".getBytes());
    }

    @Test
    public void testWriteRecordWithShardClosed() {
        TableMapping tableMapping = new TableMapping();
        tableMapping.setProjectName("test_project").setTopicName("test_topic");
        TopicWriter writer = new TopicWriter(configure, tableMapping, executor);

        ShardEntry entry1 = new ShardEntry();
        entry1.setShardId("0");
        entry1.setState(ShardState.ACTIVE);
        List<ShardEntry> shardEntryList1 = Arrays.asList(entry1);
        ListShardResult listShardResult1 = new ListShardResult();
        listShardResult1.setShards(shardEntryList1);

        ShardEntry entry2 = new ShardEntry();
        entry2.setShardId("0");
        entry2.setState(ShardState.CLOSED);
        ShardEntry entry3 = new ShardEntry();
        entry3.setShardId("1");
        entry3.setState(ShardState.ACTIVE);
        ShardEntry entry4 = new ShardEntry();
        entry4.setShardId("2");
        entry4.setState(ShardState.ACTIVE);
        List<ShardEntry> shardEntryList2 = Arrays.asList(entry2, entry3, entry4);
        ListShardResult listShardResult2 = new ListShardResult();
        listShardResult2.setShards(shardEntryList2);

        when(client.listShard(eq("test_project"), eq("test_topic")))
                .thenReturn(listShardResult1)
                .thenReturn(listShardResult2);
        ShardSealedException exception = new ShardSealedException(new DatahubClientException("shard closed"));
        doThrow(exception).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), any());
        PutRecordsByShardResult putRecordsByShardResult = new PutRecordsByShardResult();
        doReturn(putRecordsByShardResult).when(client).putRecordsByShard(eq("test_project"), eq("test_topic"), eq("1"), any());

        writer.start();
        BlobRecordData data = new BlobRecordData("test1".getBytes());
        RecordEntry recordEntry = new RecordEntry();
        recordEntry.setRecordData(data);
        writer.writeRecord(recordEntry);
        writer.syncExec();
        writer.stop();

        verify(client, times(2)).listShard(eq("test_project"), eq("test_topic"));
        ArgumentCaptor<List<RecordEntry>> argument = ArgumentCaptor.forClass(List.class);
        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("0"), argument.capture());
        List<RecordEntry> recordEntryList = argument.getValue();
        Assert.assertEquals(recordEntryList.size(), 1);
        BlobRecordData record1 = (BlobRecordData) recordEntryList.get(0).getRecordData();
        Assert.assertEquals(record1.getData(), "test1".getBytes());

        verify(client, times(1))
                .putRecordsByShard(eq("test_project"), eq("test_topic"), eq("1"), argument.capture());
        recordEntryList = argument.getValue();
        Assert.assertEquals(recordEntryList.size(), 1);
        BlobRecordData record2 = (BlobRecordData) recordEntryList.get(0).getRecordData();
        Assert.assertEquals(record2.getData(), "test1".getBytes());
    }
}
