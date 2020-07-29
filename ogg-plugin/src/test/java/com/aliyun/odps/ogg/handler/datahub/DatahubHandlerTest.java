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

import com.aliyun.datahub.client.model.CursorType;
import com.aliyun.datahub.client.model.GetRecordsResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandlerManager;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.util.DsMetric;
import org.apache.commons.io.FileUtils;
import org.dom4j.DocumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;


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

        configure = ConfigureReader.reader("src/test/resources/configure_datahub_handler.xml");

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
        columnMetaData = new ColumnMetaData("c6", 6);
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
        configure = ConfigureReader.reader("src/test/resources/configure_datahub_handler.xml");

        HandlerInfoManager.init(configure);

        RecordBuilder.init(configure);

        DataHubWriter.reInit(configure);

        OperationHandlerManager.init();

        datahubHandler.setConfigure(configure);

        DsMetric dsMetric = new DsMetric();
        datahubHandler.setHandlerMetric(dsMetric);
    }


    private RecordEntry getRecord(TableMapping tableMapping) {

        String cursor = DataHubWriter.instance().getDataHubClient().getCursor(tableMapping.getProjectName(),
                tableMapping.getTopicName(), tableMapping.getShardId(), CursorType.LATEST).getCursor();

        GetRecordsResult getRecordsResult = DataHubWriter.instance().getDataHubClient().getRecords(tableMapping.getProjectName(),
                tableMapping.getTopicName(), tableMapping.getShardId(), tableMapping.getRecordSchema(), cursor, 1);

        if (getRecordsResult.getRecordCount() > 0) {
            return getRecordsResult.getRecords().get(0);
        }

        return null;
    }

    private void myAssert(RecordEntry record, DsColumn[] columns) {
        if (record == null || columns == null) {
            Assert.fail();
        }

        TupleRecordData recordData = (TupleRecordData) record.getRecordData();

        Assert.assertEquals(recordData.getField("c1"), columns[0].getAfterValue());
        Assert.assertEquals(recordData.getField("c2"), Long.valueOf(columns[1].getAfterValue()));
        Assert.assertEquals(recordData.getField("c3"), Double.valueOf(columns[2].getAfterValue()));
        Assert.assertEquals(recordData.getField("c4"), Boolean.valueOf(columns[3].getAfterValue()));

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp ts = Timestamp.valueOf(columns[4].getAfterValue());

        long timeStamp = ts.getTime() * 1000 + (ts.getNanos() % 1000000 / 1000);
        Assert.assertEquals(recordData.getField("c5"), timeStamp);

        Assert.assertEquals(recordData.getField("c6"), new BigDecimal(columns[5].getAfterValue()));
    }

    private void myAssert(RecordEntry record, DsColumn[] columns, boolean isDateFormat) {
        if (record == null || columns == null) {
            Assert.fail();
        }

        TupleRecordData recordData = (TupleRecordData) record.getRecordData();

        Assert.assertEquals(recordData.getField("c1"), columns[0].getAfterValue());
        Assert.assertEquals(recordData.getField("c2"), Long.valueOf(columns[1].getAfterValue()));
        Assert.assertEquals(recordData.getField("c3"), Double.valueOf(columns[2].getAfterValue()));
        Assert.assertEquals(recordData.getField("c4"), Boolean.valueOf(columns[3].getAfterValue()));
        Assert.assertEquals(recordData.getField("c5"), Long.valueOf(columns[4].getAfterValue()));
        Assert.assertEquals(recordData.getField("c6"), new BigDecimal(columns[5].getAfterValue()));
    }


    @Test
    public void testNormal() {
        configure.setDirtyDataContinue(false);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("2019-12-12 12:00:00.123456789");
        columns[5] = new DsColumnAfterValue("1.23456789");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person"));
        myAssert(record, columns);
    }


    @Test
    public void testDirtyNotContinue() {
        configure.setDirtyDataContinue(false);
        DataHubWriter.reInit(configure);
        datahubHandler.transactionBegin(e, dsTransaction);


        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("4");
        columns[5] = new DsColumnAfterValue("1.23456789");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        datahubHandler.transactionCommit(e, dsTransaction);

        Assert.assertEquals(GGDataSource.Status.ABEND, status);

    }

    @Test
    public void testDirtyContinue() throws IOException {
        configure.setDirtyDataContinue(true);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("4");
        columns[5] = new DsColumnAfterValue("1.23456789");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        datahubHandler.transactionCommit(e, dsTransaction);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        File file = new File(configure.getDirtyDataFile());

        FileUtils.forceDelete(file);
    }

    @Test(testName = "指定shard id写入")
    public void testShardId() {
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardIds(Arrays.asList("1"));
        configure.getTableMapping("ogg_test.t_person").setSetShardId(true);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("2019-12-12 12:00:00");
        columns[5] = new DsColumnAfterValue("1.23456789");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person"));

        myAssert(record, columns);

    }

    @Test
    public void testTimestamp() {
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setShardIds(Arrays.asList("1"));
        configure.getTableMapping("ogg_test.t_person").setSetShardId(true);
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c5").setIsDateFormat(false);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("1575983705123");
        columns[5] = new DsColumnAfterValue("1.23456789");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person"));

        myAssert(record, columns, false);
    }

    @Test
    public void testBatchSize() {
        configure.setDirtyDataContinue(true);

        configure.setBatchSize(0);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("2019-12-12 20:30:00");
        columns[5] = new DsColumnAfterValue("1.23456789");

        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person"));

        myAssert(record, columns);

        datahubHandler.transactionCommit(e, dsTransaction);
    }

    @Test(testName = "hash写入")
    public void testHashShard(){
        configure.setDirtyDataContinue(true);

        configure.getTableMapping("ogg_test.t_person").setSetShardId(false);
        configure.getTableMapping("ogg_test.t_person").setShardIds(Arrays.asList("0"));
        configure.getTableMapping("ogg_test.t_person").setShardHash(true);
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c1").setIsShardColumn(true);
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c2").setIsShardColumn(true);
        configure.getTableMapping("ogg_test.t_person").getColumnMappings().get("c3").setIsShardColumn(true);

        DataHubWriter.reInit(configure);

        datahubHandler.transactionBegin(e, dsTransaction);

        DsColumn[] columns = new DsColumn[6];
        columns[0] = new DsColumnAfterValue("testNormal");
        columns[1] = new DsColumnAfterValue("123456");
        columns[2] = new DsColumnAfterValue("3.1415926");
        columns[3] = new DsColumnAfterValue("true");
        columns[4] = new DsColumnAfterValue("2019-12-12:12:00:00");
        columns[5] = new DsColumnAfterValue("1.23456789");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2016-05-13 19:15:15.010", 0L, 0L, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        datahubHandler.transactionCommit(e, dsTransaction);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_person"));

    }
}

