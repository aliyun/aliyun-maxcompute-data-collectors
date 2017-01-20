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


import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.DsColumnAfterValue;
import com.goldengate.atg.datasource.DsOperation;
import com.goldengate.atg.datasource.DsRecord;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by lyf0429 on 16/5/25.
 */
public class BadOperateWriterTest {

    @Test
    public void testCheckFileSize() throws IOException {

        for(int i = 0 ; i < 2; i++){
            String content = "hello,world";

            if(i == 1){
                content = "hello,liangyf";
            }

            String fileName = "1.txt";


            File file = new File(fileName);

            FileOutputStream outputStream = FileUtils.openOutputStream(file);

            outputStream.write(content.getBytes());
            outputStream.flush();
            outputStream.close();

            BadOperateWriter.checkFileSize(fileName, 1);

            Assert.assertTrue(!file.exists());

            BufferedReader br=new BufferedReader(new FileReader(fileName+".bak"));

            String line="";
            StringBuffer  buffer = new StringBuffer();
            while((line=br.readLine())!=null){
                buffer.append(line);
            }

            br.close();

            String fileContent = buffer.toString();

            Assert.assertEquals(content, fileContent);
        }


        FileUtils.deleteQuietly(new File("1.txt"));
        FileUtils.deleteQuietly(new File("1.txt.bak"));
    }

    @Test(testName = "测试写入op")
    public void testWriteOp(){
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

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        Op op = new Op(dsOperation, tableMetaData, null);

        BadOperateWriter.write(op,
                "ogg_test.t_person",
                "t_person",
                "op.txt",
                10000,
                "op error");

        FileUtils.deleteQuietly(new File("op.txt"));
    }

    @Test(testName = "测试写入record")
    public void testWriteRecord(){
        DatahubConfiguration datahubConfiguration = new DatahubConfiguration(new AliyunAccount("YOUR_DATAHUB_ACCESS_ID", "YOUR_DATAHUB_ACCESS_KEY"), "YOUR_DATAHUB_ENDPOINT");
        Project project = Project.Builder.build("YOUR_DATAHUB_PROJECT", datahubConfiguration);

        Topic topic = project.getTopic("ogg_test_normal");
        RecordEntry recordEntry = new RecordEntry(topic.getRecordSchema());
        recordEntry.setShardId("1");

        recordEntry.setString("c1", "c1");
        recordEntry.setBigint("c2", 2L);
        recordEntry.setDouble("c3", 2.0);
        recordEntry.setTimeStamp("c4", 1000L);
        recordEntry.setBoolean("c5", true);


        BadOperateWriter.write(recordEntry,
                "ogg_test.t_person",
                "t_person",
                "record.txt",
                10000,
                "record error");

        FileUtils.deleteQuietly(new File("record.txt"));


    }


}
