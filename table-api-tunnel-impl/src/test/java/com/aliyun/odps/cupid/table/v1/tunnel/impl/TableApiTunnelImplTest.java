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

package com.aliyun.odps.cupid.table.v1.tunnel.impl;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.aliyun.odps.cupid.table.v1.util.Options;
import com.aliyun.odps.cupid.table.v1.util.TableUtils;
import com.aliyun.odps.cupid.table.v1.writer.*;
import com.aliyun.odps.data.ArrayRecord;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableApiTunnelImplTest {
    private static final String provider = "tunnel";
    private static final String project = "";
    private static final String accessId = "";
    private static final String accessKey = "";
    private static final String endPoint = "";
    private static final String table = "table_api_tunnel_impl_test";
    private static final String partitionedTable = "table_api_tunnel_imple_test_partitioned";
    private static final String partitionName = "pcol1";
    private static final String partitionValue1 = "foo";
    private static final String partitionValue2 = "bar";
    private static Options options;

    @BeforeClass
    public static void setUp() throws OdpsException {
        Odps odps = new Odps(new AliyunAccount(accessId, accessKey));
        odps.setDefaultProject(project);
        odps.setEndpoint(endPoint);
        odps.tables().delete(table, true);
        odps.tables().delete(partitionedTable, true);

        // Create table for test
        TableSchema schema = new TableSchema();
        schema.addColumn(new Column("col1", OdpsType.STRING));
        odps.tables().create(table, schema);

        // Create partition table for test
        TableSchema partitionedSchema = new TableSchema();
        partitionedSchema.addColumn(new Column("col1", OdpsType.STRING));
        partitionedSchema.addPartitionColumn(new Column(partitionName, OdpsType.STRING));
        odps.tables().create(partitionedTable, partitionedSchema);

        // Create partition for test
        String partitionSpecStr = partitionName + "=" + partitionValue1;
        PartitionSpec partitionSpec = new PartitionSpec(partitionSpecStr);
        odps.tables().get(partitionedTable).createPartition(partitionSpec);
        partitionSpecStr = partitionName + "=" + partitionValue2;
        partitionSpec = new PartitionSpec(partitionSpecStr);
        odps.tables().get(partitionedTable).createPartition(partitionSpec);

        Options.OptionsBuilder builder = new Options.OptionsBuilder();
        builder.accessId(accessId);
        builder.accessKey(accessKey);
        builder.project(project);
        builder.endpoint(endPoint);
        options = builder.build();
    }

    @Test
    public void test() throws IOException, ClassNotFoundException {
        writeTable();
        readTable();
    }

    @Test
    public void testPartitionedTable() throws IOException, ClassNotFoundException {
        Map<String, String> partitionSpec1 = new HashMap<>();
        partitionSpec1.put(partitionName, partitionValue1);
        Map<String, String> partitionSpec2 = new HashMap<>();
        partitionSpec2.put(partitionName, partitionValue2);

        writePartitionedTable(partitionSpec1);
        writePartitionedTable(partitionSpec2);

        List<Map<String, String>> partitionSpecs = new ArrayList<>();
        partitionSpecs.add(partitionSpec1);
        partitionSpecs.add(partitionSpec2);
        readPartitionedTable(partitionSpecs);
    }

    private void writeTable() throws ClassNotFoundException, IOException {
        TableWriteSessionBuilder writeSessionBuilder = new TableWriteSessionBuilder(
                provider, project, table);
        writeSessionBuilder.options(options);
        TableWriteSession writeSession = writeSessionBuilder.build();
        WriteSessionInfo info = writeSession.getOrCreateSessionInfo();

        write(info, 0);
        write(info, 1);

        writeSession.commitTable();
    }

    private void writePartitionedTable(Map<String, String> partitionSpec)
        throws ClassNotFoundException, IOException {
        TableWriteSessionBuilder writeSessionBuilder = new TableWriteSessionBuilder(
            provider, project, partitionedTable);
        TableWriteSession writeSession = writeSessionBuilder
                .partitionSpec(partitionSpec)
                .options(options)
                .build();
        WriteSessionInfo info = writeSession.getOrCreateSessionInfo();

        write(info, 0);
        write(info, 1);

        writeSession.commitTable();
    }

    private void write(WriteSessionInfo info, int index) throws ClassNotFoundException, IOException {
        FileWriter<ArrayRecord> writer = new FileWriterBuilder(info, index).buildRecordWriter();

        ArrayRecord record = new ArrayRecord(TableUtils.toTableSchema(info));

        record.set(0, "foo" + index);
        writer.write(record);

        writer.close();
        writer.commit();
    }

    private void readTable() throws ClassNotFoundException, IOException {
        TableReadSessionBuilder readSessionBuilder = new TableReadSessionBuilder(
                provider, project, table);
        TableReadSession readSession = readSessionBuilder
                .readDataColumns(RequiredSchema.all())
                .options(options)
                .splitBySize(10)
                .build();

        InputSplit[] inputSplits = readSession.getOrCreateInputSplits();
        Assert.assertEquals(1, inputSplits.length);
        for (InputSplit inputSplit : inputSplits) {
            read(inputSplit);
        }
    }

    private void readPartitionedTable(List<Map<String, String>> partitionSpecs) throws ClassNotFoundException, IOException {
        TableReadSessionBuilder readSessionBuilder = new TableReadSessionBuilder(
            provider, project, partitionedTable);
        List<PartitionSpecWithBucketFilter> partitionSpecWithBucketFilters = new ArrayList<>();
        for (Map<String, String> partitionSpec : partitionSpecs) {
            partitionSpecWithBucketFilters.add(new PartitionSpecWithBucketFilter(partitionSpec));
        }
        TableReadSession readSession = readSessionBuilder
                .readDataColumns(RequiredSchema.all())
                .readPartitions(partitionSpecWithBucketFilters)
                .splitBySize(10)
                .options(options)
                .build();

        InputSplit[] inputSplits = readSession.getOrCreateInputSplits();
        Assert.assertEquals(2, inputSplits.length);
        for (InputSplit inputSplit : inputSplits) {
            read(inputSplit);
        }
    }

    private void read(InputSplit inputSplit) throws ClassNotFoundException, IOException {
        SplitReader<ArrayRecord> reader = new SplitReaderBuilder(inputSplit).buildRecordReader();

        System.out.println("Table: " + inputSplit.getTable());
        if (inputSplit.getPartitionSpec() != null) {
            System.out.println("Partition: " +
                               Util.toOdpsPartitionSpec(inputSplit.getPartitionSpec()).toString());
        }
        ArrayRecord record = null;
        while(reader.hasNext()) {
            record = reader.next();
            System.out.println(record.getString(0));
        }
    }
}
