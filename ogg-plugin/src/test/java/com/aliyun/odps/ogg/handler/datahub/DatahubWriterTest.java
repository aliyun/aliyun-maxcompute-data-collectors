package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DatahubWriterTest {

    @Before
    public void setUpBeforeTest() {
        Configure configure = new Configure();
        configure.setDatahubEndpoint("http://10.101.214.153:9094");
        configure.setDatahubAccessId("1");
        configure.setDatahubAccessKey("2");

        TableMapping tableMapping = new TableMapping();
        tableMapping.setOracleFullTableName("oracle");
        tableMapping.setProjectName("blink_test");
        tableMapping.setTopicName("test_tuple");

        Map<String, String> mapp = new HashMap<>();
        mapp.putIfAbsent("f1", "f1");

        Map<String, ColumnMapping> mappp = new HashMap<>();
        mappp.putIfAbsent("f", new ColumnMapping());

        tableMapping.setConstColumnMappings(mapp);
        tableMapping.setColumnMappings(mappp);
        configure.addTableMapping(tableMapping);
        DataHubWriter.reInit(configure);
    }

    @After
    public void tearDownAfterTest() {
    }

    private RecordEntry getRecord(int sequence) {
        RecordSchema recordSchema = new RecordSchema();
        recordSchema.addField(new Field("f1", FieldType.STRING));
        recordSchema.addField(new Field("f2", FieldType.BIGINT));
        recordSchema.addField(new Field("f3", FieldType.DOUBLE));
        recordSchema.addField(new Field("f4", FieldType.BOOLEAN));
        recordSchema.addField(new Field("f5", FieldType.TIMESTAMP));
        recordSchema.addField(new Field("f6", FieldType.DECIMAL));
        RecordEntry recordEntry = new RecordEntry();
        TupleRecordData recordData = new TupleRecordData(recordSchema);
        recordData.setField(0, "f" + sequence);
        recordData.setField(1, sequence);
        recordEntry.setRecordData(recordData);

        return recordEntry;
    }

    @Test
    public void testTuple() throws InterruptedException {
        for (int i = 0; i < 5000; ++i) {
            DataHubWriter.instance().addRecord("oracle", getRecord(i));
            Thread.sleep(100);
        }
    }
}
