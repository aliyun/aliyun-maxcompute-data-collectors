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

import com.aliyun.datahub.model.GetCursorRequest;
import com.aliyun.datahub.model.GetCursorResult;
import com.aliyun.datahub.model.GetRecordsResult;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.wrapper.Topic;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.PluginStatictics;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandlerManager;
import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.meta.ColumnMetaData;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.goldengate.atg.datasource.meta.TableMetaData;
import com.goldengate.atg.datasource.meta.TableName;
import maxcompute.data.collectors.common.datahub.RecordUtil;
import org.dom4j.DocumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * Created by lyf0429 on 16/5/20.lic
 */
public class DatahubHandlerTest {
    private DatahubHandler datahubHandler;

    private Configure configure;

    private DsEvent e;

    private DsTransaction dsTransaction;

    private TableName tableName;

    @BeforeClass
    public void init() throws DocumentException {
        datahubHandler = new DatahubHandler();

        datahubHandler.setState(DataSourceListener.State.READY);

        configure = ConfigureReader.reader("src/test/resources/configure.xml");

        datahubHandler.setConfigure(configure);

        RecordBuilder.init(configure);

        DataHubWriter.init(configure);

        OperationHandlerManager.init();

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



        tableName = new TableName("ogg_test.t_person");

        TableMetaData tableMetaData = new TableMetaData(tableName, columnMetaDatas);

        DsMetaData dsMetaData = new DsMetaData();
        dsMetaData.setTableMetaData(tableMetaData);


        GGTranID ggTranID = GGTranID.getID(0l, 0l);
        dsTransaction = new DsTransaction(ggTranID);

        e = new DsEventManager.TxEvent(dsTransaction, dsMetaData, "");
    }

    @BeforeMethod
    public void reInit() throws DocumentException {
        configure = ConfigureReader.reader("src/test/resources/configure.xml");

        datahubHandler.setConfigure(configure);

        DataHubWriter.reInit(configure);
    }


    private RecordEntry getRecord(Topic topic, String shardId){
        String cursor = topic.getCursor(shardId, GetCursorRequest.CursorType.LATEST);

        GetRecordsResult getRecordsResult = topic.getRecords(shardId, cursor, 1);

        if(getRecordsResult.getRecordCount() > 0){
            return  getRecordsResult.getRecords().get(0);
        }

        return  null;
    }

    private void myAssert(RecordEntry record, DsColumn[] columns, boolean isDateFormat, String dateFormat){
        if(record == null || columns == null){
            Assert.assertTrue(false);
        }

        Assert.assertEquals(record.getString("c1"), columns[0].getAfterValue());
        Assert.assertEquals(record.getBigint("c2"), Long.valueOf(columns[1].getAfterValue()));
        Assert.assertEquals(record.getDouble("c3"), Double.valueOf(columns[2].getAfterValue()));

        if(isDateFormat){
            if(dateFormat == null){
                dateFormat = "yyyy-MM-dd HH:mm:ss";
            }

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);

            try {
                Long timeStamp = simpleDateFormat.parse(columns[3].getAfterValue()).getTime();
                Assert.assertEquals(record.getTimeStamp("c4"), timeStamp);
            } catch (ParseException e1) {
                Assert.assertTrue(false);
            }
        } else{
            Assert.assertEquals(record.getTimeStamp("c4"), Long.valueOf(columns[3].getAfterValue()));
        }

        if(RecordUtil.trueString.contains(columns[4].getAfterValue().toLowerCase())){
            Assert.assertEquals(record.getBoolean("c5"), Boolean.TRUE);
        } else if(RecordUtil.falseString.contains(columns[4].getAfterValue().toLowerCase())){
            Assert.assertEquals(record.getBoolean("c5"), Boolean.FALSE);
        } else {
            Assert.assertEquals(record.getBoolean("c5"), null);
        }
    }


    @Test
    public void testNormal(){
        configure.setDirtyDataContinue(false);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("6");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                configure.getTableMapping("ogg_test.t_person").getShardId());

        myAssert(record, columns, true, null);
    }


    @Test
    public void testDirtyNotContinue(){
        configure.setDirtyDataContinue(false);

        DataHubWriter.reInit(configure);


        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testDirtyNotContinue");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("4");
        columns[4] = new DsColumnAfterValue("5");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        datahubHandler.transactionCommit(e, dsTransaction);

        Assert.assertEquals(GGDataSource.Status.ABEND, status);

    }

    @Test
    public void testDirtyContinue(){
        configure.setDirtyDataContinue(true);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testDirtyContinue");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("4");
        columns[4] = new DsColumnAfterValue("5");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        datahubHandler.transactionCommit(e, dsTransaction);

        Assert.assertEquals(GGDataSource.Status.OK, status);

    }

    @Test(testName = "指定shard id写入")
    public void testShardId(){
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardId("0");

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue("testShardId");
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("TRUE");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                configure.getTableMapping("ogg_test.t_person").getShardId());

        myAssert(record, columns, true, null);

    }

    @Test(testName = "hash写入")
    public void testHashShard(){
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardId(null);
        configure.getTableMapping("ogg_test.t_person").setIsShardHash(true);
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c1").setIsShardColumn(true);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        String id = UUID.randomUUID().toString();

        String shardId = "0";

        {
            DsColumn[] columns = new DsColumn[5];
            columns[0] = new DsColumnAfterValue(id);
            columns[1] = new DsColumnAfterValue("1");
            columns[2] = new DsColumnAfterValue("1");
            columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
            columns[4] = new DsColumnAfterValue("TRUE");


            DsRecord dsRecord = new DsRecord(columns);

            DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0l, 0l, dsRecord);

            GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

            Assert.assertEquals(GGDataSource.Status.OK, status);

            datahubHandler.transactionCommit(e, dsTransaction);

            RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    "0");

            try {
                myAssert(record, columns, true, null);
            } catch (AssertionError assertionError){
                shardId = "1";
            }

        }


        {
            DsColumn[] columns = new DsColumn[5];
            columns[0] = new DsColumnAfterValue(id);
            columns[1] = new DsColumnAfterValue("2");
            columns[2] = new DsColumnAfterValue("2");
            columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
            columns[4] = new DsColumnAfterValue("TRUE");


            DsRecord dsRecord = new DsRecord(columns);

            DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0l, 0l, dsRecord);

            GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

            Assert.assertEquals(GGDataSource.Status.OK, status);

            datahubHandler.transactionCommit(e, dsTransaction);


            RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    shardId);

            myAssert(record, columns, true, null);
        }
    }

    @Test(testName = "轮询写入")
    public void testPollingShard(){
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardId(null);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns1 = new DsColumn[5];

        {
            String id = UUID.randomUUID().toString();

            columns1[0] = new DsColumnAfterValue(id);
            columns1[1] = new DsColumnAfterValue("1");
            columns1[2] = new DsColumnAfterValue("1");
            columns1[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
            columns1[4] = new DsColumnAfterValue("TRUE");


            DsRecord dsRecord = new DsRecord(columns1);

            DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0l, 0l, dsRecord);

            GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

            Assert.assertEquals(GGDataSource.Status.OK, status);
        }

        DsColumn[] columns2 = new DsColumn[5];

        {
            String id = UUID.randomUUID().toString();

            columns2[0] = new DsColumnAfterValue(id);
            columns2[1] = new DsColumnAfterValue("2");
            columns2[2] = new DsColumnAfterValue("2");
            columns2[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
            columns2[4] = new DsColumnAfterValue("TRUE");


            DsRecord dsRecord = new DsRecord(columns2);

            DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0l, 0l, dsRecord);

            GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

            Assert.assertEquals(GGDataSource.Status.OK, status);
        }

        datahubHandler.transactionCommit(e, dsTransaction);

        try{
            RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    "0");

            myAssert(record, columns1, true, null);

            record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    "1");

            myAssert(record, columns2, true, null);
        } catch (AssertionError assertionError){
            RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    "1");

            myAssert(record, columns1, true, null);

            record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                    "0");

            myAssert(record, columns2, true, null);
        }
    }

    @Test
    public void testTimestamp(){
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardId("1");
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c4").setIsDateFormat(false);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        String id = UUID.randomUUID().toString();

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue(id);
        columns[1] = new DsColumnAfterValue("2");
        columns[2] = new DsColumnAfterValue("3");
        columns[3] = new DsColumnAfterValue("100");
        columns[4] = new DsColumnAfterValue("TRUE");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                configure.getTableMapping("ogg_test.t_person").getShardId());

        myAssert(record, columns, false, null);
    }

    @Test
    public void testBatchSize(){
        configure.setDirtyDataContinue(true);

        configure.setBatchSize(0);

        datahubHandler.transactionBegin(e, dsTransaction);

        String id = UUID.randomUUID().toString();

        DsColumn[] columns = new DsColumn[5];
        columns[0] = new DsColumnAfterValue(id);
        columns[1] = new DsColumnAfterValue("1");
        columns[2] = new DsColumnAfterValue("1");
        columns[3] = new DsColumnAfterValue("2016-05-20 09:00:00");
        columns[4] = new DsColumnAfterValue("TRUE");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010",0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person").getTopic(),
                configure.getTableMapping("ogg_test.t_person").getShardId());

        myAssert(record, columns, true, null);

        datahubHandler.transactionCommit(e, dsTransaction);
    }

}
