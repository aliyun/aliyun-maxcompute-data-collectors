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

package demo;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.aliyun.odps.cupid.table.v1.writer.*;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.type.TypeInfoFactory;
import demo.memory.MemoryStore;

import java.io.IOException;

public class Job {

    private String provider;
    private String project;
    private String table;

    public Job(String provider, String project, String table) {
        this.provider = provider;
        this.project = project;
        this.table = table;
    }

    public void run() {
        try {
            MemoryStore.createProject(project);
            MemoryStore.createTable(project, table);

            writeTable();

            readTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeTable() throws ClassNotFoundException, IOException {
        TableWriteSession session = new TableWriteSessionBuilder(provider, project, table)
                .tableSchema(getTableSchema())
                .build();
        WriteSessionInfo info = session.getOrCreateSessionInfo();

        writeData(info, 0);
        writeData(info, 1);

        session.commitTable();
    }

    private void writeData(WriteSessionInfo info,
                           int index) throws ClassNotFoundException, IOException {
        FileWriter<ArrayRecord> writer = new FileWriterBuilder(info, index)
                .buildRecordWriter();
        ArrayRecord record = new ArrayRecord(getOdpsColumns());

        record.set(0, index * 10);
        record.set(1, "hello " + index);
        writer.write(record);

        record.set(0, index * 10 + 1);
        record.set(1, "world " + index);
        writer.write(record);

        writer.close();
        writer.commit();
    }

    private void readTable() throws ClassNotFoundException, IOException {
        TableReadSession session = new TableReadSessionBuilder(provider, project, table)
                .readDataColumns(RequiredSchema.all())
                .build();
        InputSplit[] inputSplits = session.getOrCreateInputSplits(256);

        for (InputSplit inputSplit : inputSplits) {
            readSplit(inputSplit);
        }
    }

    private void readSplit(InputSplit inputSplit) throws ClassNotFoundException, IOException {
        SplitReader<ArrayRecord> reader = new SplitReaderBuilder(inputSplit)
                .buildRecordReader();

        while (reader.hasNext()) {
            ArrayRecord record = reader.next();
            System.out.println(record.getInt(0));
            System.out.println(record.getString(1));
        }
    }

    private TableSchema getTableSchema() {
        TableSchema tableSchema = new TableSchema();
        tableSchema.addColumn(new Column("id", TypeInfoFactory.INT));
        tableSchema.addColumn(new Column("value", TypeInfoFactory.STRING));
        return tableSchema;
    }

    private Column[] getOdpsColumns() {
        return getTableSchema().getColumns().toArray(new Column[0]);
    }

    public static void main(String[] args) {
        Job job = new Job("memory", "cupid", "test");
        job.run();
    }
}
