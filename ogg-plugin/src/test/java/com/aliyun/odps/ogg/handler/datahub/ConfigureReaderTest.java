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

import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import com.google.common.collect.Sets;
import org.dom4j.DocumentException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;


/**
 * Created by lyf0429 on 16/5/24.
 */
public class ConfigureReaderTest {
    @Test
    public void testConfigNormal() throws DocumentException {
        Configure configure = ConfigureReader.reader("src/test/resources/configure.xml");

        Assert.assertEquals(configure.getOracleSid(), "100");
        Assert.assertEquals(configure.getDatahubEndpoint(), "YOUR_DATAHUB_ENDPOINT");
        Assert.assertEquals(configure.getDatahubAccessId(), "YOUR_DATAHUB_ACCESS_ID");
        Assert.assertEquals(configure.getDatahubAccessKey(), "YOUR_DATAHUB_ACCESS_KEY");
        Assert.assertTrue(configure.isEnablePb());
        Assert.assertEquals(configure.getCompressType(), "LZ4");
        Assert.assertEquals(configure.getBatchSize(), 1111);
        Assert.assertEquals(configure.getBatchTimeoutMs(), 5000);
        Assert.assertTrue(configure.isDirtyDataContinue());
        Assert.assertEquals(configure.getDirtyDataFile(), "datahub_ogg_plugin.dirty.test");
        Assert.assertEquals(configure.getDirtyDataFileMaxSize(), 200 * 1000000);
        Assert.assertEquals(configure.getRetryTimes(), 35);
        Assert.assertEquals(configure.getRetryIntervalMs(), 1000);
        Assert.assertEquals(configure.getCheckPointFileName(), "datahub_ogg_plugin.chk.test");
        Assert.assertEquals(configure.getCharsetName(), "utf-8");
        Assert.assertTrue(configure.isCheckPointFileDisable());
        Assert.assertTrue(configure.isCommitFlush());
        Assert.assertFalse(configure.isReportMetric());
        Assert.assertEquals(configure.getReportMetricIntervalMs(), 5 * 60 * 1000);

        Map<String, TableMapping> tableMappingMap = configure.getTableMappings();
        Assert.assertEquals(tableMappingMap.size(), 2);
        Assert.assertEquals(tableMappingMap.keySet(), Sets.newHashSet("ogg_test.test2", "t_schema.t_person"));


        TableMapping table1 = configure.getTableMapping("t_schema.t_person");

        Assert.assertEquals(table1.getOracleSchema(), "t_schema");
        Assert.assertEquals(table1.getOracleTableName(), "t_person");
        Assert.assertEquals(table1.getOracleFullTableName(), "t_schema.t_person");
        Assert.assertEquals(table1.getProjectName(), "t_project");
        Assert.assertEquals(table1.getTopicName(), "ogg_test_normal");
        Assert.assertEquals(table1.getAccessId(), "accessId");
        Assert.assertEquals(table1.getAccessKey(), "accessKey");
        Assert.assertEquals(table1.getShardIds(), Arrays.asList("0", "1"));
        Assert.assertNull(table1.getRecordSchema());
        Assert.assertEquals(table1.getRowIdColumn(), "row_id");
        Assert.assertEquals(table1.getcTypeColumn(), "ctype");
        Assert.assertEquals(table1.getcTimeColumn(), "ctime");
        Assert.assertEquals(table1.getcIdColumn(), "cid");
        Map<String, String> map = table1.getConstColumnMappings();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("const1"), "3");
        Assert.assertEquals(map.get("const4"), "abcd");
        Assert.assertEquals(table1.getcIdColumn(), "cid");


        Map<String, ColumnMapping> columns = table1.getColumnMappings();
        Assert.assertEquals(columns.size(), 2);
        Assert.assertEquals(columns.keySet(), Sets.newHashSet("c1", "c2"));
        ColumnMapping column1 = columns.get("c1");
        Assert.assertEquals(column1.getDest(), "c1");
        Assert.assertNull(column1.getDestOld());
        Assert.assertFalse(column1.isShardColumn());
        Assert.assertTrue(column1.isDateFormat());
        Assert.assertFalse(column1.isKeyColumn());
        Assert.assertTrue(column1.isDefaultCharset());
        ColumnMapping column2 = columns.get("c2");
        Assert.assertEquals(column2.getDest(), "c2");
        Assert.assertEquals(column2.getDestOld(), "c2_old");
        Assert.assertTrue(column2.isShardColumn());
        Assert.assertFalse(column2.isDateFormat());
        Assert.assertEquals(column2.getDateFormat(), "yyyy-MM-dd:HH:mm:ss");
        Assert.assertFalse(column2.isKeyColumn());
        Assert.assertFalse(column2.isDefaultCharset());


        TableMapping table2 = configure.getTableMapping("ogg_test.test2");

        Assert.assertEquals(table2.getOracleSchema(), "ogg_test");
        Assert.assertEquals(table2.getOracleTableName(), "test2");
        Assert.assertEquals(table2.getOracleFullTableName(), "ogg_test.test2");
        Assert.assertEquals(table2.getProjectName(), "YOUR_DATAHUB_PROJECT");
        Assert.assertEquals(table2.getTopicName(), "ogg_test_normal");
        Assert.assertEquals(table2.getAccessId(), "YOUR_DATAHUB_ACCESS_ID");
        Assert.assertEquals(table2.getAccessKey(), "YOUR_DATAHUB_ACCESS_KEY");
        Assert.assertTrue(table2.getShardIds().isEmpty());
        Assert.assertNull(table2.getRecordSchema());
        Assert.assertNull(table2.getRowIdColumn());
        Assert.assertNull(table2.getcTypeColumn());
        Assert.assertNull(table2.getcTimeColumn());
        Assert.assertNull(table2.getcIdColumn());
        Assert.assertTrue(table2.getConstColumnMappings().isEmpty());

        columns = table2.getColumnMappings();
        Assert.assertEquals(columns.size(), 2);
        column1 = columns.get("c1");
        Assert.assertEquals(column1.getDest(), "c1");
        Assert.assertEquals(column1.getDateFormat(), "yyyy-MM-dd");
        Assert.assertNull(column1.getDestOld());
        column2 = columns.get("c2");
        Assert.assertNull(column2.getDateFormat());
        Assert.assertNull(column2.getSimpleDateFormat());

    }

    @Test
    public void testConfigMissTopic() throws DocumentException {
        try {
            ConfigureReader.reader("src/test/resources/configure_miss_topic.xml");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "mappings.mapping.datahubTopic is null");
        }
    }

    @Test
    public void testConfigMissDefaultDataHub() throws DocumentException {
        try {
            ConfigureReader.reader("src/test/resources/configure_miss_default_datahub.xml");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "defaultDatahubConfigure is null");
        }
    }

    @Test
    public void testConfigMissDefaultOracle() throws DocumentException {
        try {
            ConfigureReader.reader("src/test/resources/configure_miss_default_oracle.xml");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "defaultOracleConfigure is null");
        }
    }

    @Test
    public void testConfigMissMapping() throws DocumentException {
        try {
            ConfigureReader.reader("src/test/resources/configure_miss_map.xml");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "mappings.mapping is null");
        }
    }

    @Test(enabled = false)
    public void test() throws DocumentException {

        try {
            ConfigureReader.reader("src/test/resources/configure_datahub_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }

        try {
            ConfigureReader.reader("src/test/resources/configure_topic_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }

        try {
            ConfigureReader.reader("src/test/resources/configure_field_error.xml");
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }

        try {
            Configure configure = ConfigureReader.reader("src/test/resources/configure_no_active_shard.xml");
            DataHubWriter.reInit(configure);
            Assert.assertTrue(false);
        } catch (RuntimeException e) {
            Assert.assertTrue(true);
        }
    }


}
