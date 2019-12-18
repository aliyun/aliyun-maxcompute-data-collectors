package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandlerManager;
import oracle.goldengate.datasource.*;
import oracle.goldengate.datasource.meta.ColumnMetaData;
import oracle.goldengate.datasource.meta.DsMetaData;
import oracle.goldengate.datasource.meta.TableMetaData;
import oracle.goldengate.datasource.meta.TableName;
import oracle.goldengate.util.DsMetric;
import org.dom4j.DocumentException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class DataHubBlobTest {


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


        tableName = new TableName("ogg_test.t_blob");

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
                tableMapping.getTopicName(), tableMapping.getShardId(), cursor, 1);

        if (getRecordsResult.getRecordCount() > 0) {
            return getRecordsResult.getRecords().get(0);
        }

        return null;
    }


    @Test
    public void TestBlobNormal(){
        configure.setDirtyDataContinue(true);

        datahubHandler.transactionBegin(e, dsTransaction);


        DsColumn[] columns = new DsColumn[1];
        columns[0] = new DsColumnAfterValue("testNormal");


        DsRecord dsRecord = new DsRecord(columns);

        DsOperation dsOperation = new DsOperation(tableName, DsOperation.OpType.DO_INSERT, "2019-12-12 20:30:00.010", 0l, 0l, dsRecord);

        GGDataSource.Status status = datahubHandler.operationAdded(e, dsTransaction, dsOperation);

        datahubHandler.transactionCommit(e, dsTransaction);

        Assert.assertEquals(GGDataSource.Status.OK, status);

        RecordEntry record = this.getRecord(configure.getTableMapping("ogg_test.t_blob"));

        BlobRecordData recordData = (BlobRecordData) record.getRecordData();
        System.out.println(new String(recordData.getData()));
    }
}
