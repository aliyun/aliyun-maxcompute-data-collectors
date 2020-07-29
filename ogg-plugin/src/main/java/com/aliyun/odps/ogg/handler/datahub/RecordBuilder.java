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
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.FieldType;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import oracle.goldengate.datasource.DsToken;
import oracle.goldengate.datasource.adapt.Op;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;


public class RecordBuilder {
    private final static Logger logger = LoggerFactory.getLogger(RecordBuilder.class);
    private static RecordBuilder recordBuilder;

    private Configure configure;
    private DatahubClient client;

    public static RecordBuilder instance() {
        return recordBuilder;
    }

    public static void init(Configure configure) {
        if (recordBuilder == null) {
            recordBuilder = new RecordBuilder(configure);
        }
        recordBuilder.start();
    }

    public static void destroy() {
        if (recordBuilder != null) {
            recordBuilder.stop();
        }
        recordBuilder = null;
    }

    public void start() {
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableMapping tableMapping = entry.getValue();
            for (TableRecordBuilder tableRecordBuilder : tableMapping.getTableRecordBuilders()) {
                tableRecordBuilder.start();
            }
            tableMapping.getTopicWriter().start();
        }
    }

    public void stop() {
        flushAll();
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableMapping tableMapping = entry.getValue();
            for (TableRecordBuilder tableRecordBuilder : entry.getValue().getTableRecordBuilders()) {
                tableRecordBuilder.stop();
            }
            tableMapping.getTopicWriter().stop();
        }
    }

    public boolean buildRecord(Op op, String opType, String recordId) {
        String oracleFullTableName = op.getTableName().getFullName().toLowerCase();
        TableMapping tableMapping = configure.getTableMapping(oracleFullTableName);

        if (tableMapping != null) {
            List<TableRecordBuilder> recordBuilders = tableMapping.getTableRecordBuilders();
            if (recordBuilders.size() == 1) {
                return recordBuilders.get(0).addRecord(op, opType, recordId);
            } else if (recordBuilders.size() > 1) {
                DsToken token = op.getRecord().getUserToken(Constant.ROWID_TOKEN);
                if (token.isSet()) {
                    int index = ((token.getValue().hashCode() % recordBuilders.size()) + recordBuilders.size()) % recordBuilders.size();
                    return recordBuilders.get(index).addRecord(op, opType, recordId);
                } else {
                    logger.error("BuildRecord failed, build speed > 1, but oracle table token TKN-ROWID is not set, can not get oracle rowid, table: {}",
                            tableMapping.getOracleFullTableName());
                    throw new RuntimeException("BuildRecord failed, build speed > 1, but oracle table token TKN-ROWID is not set, can not get oracle rowid");
                }
            } else {
                logger.error("RecordBuilder list is empty, table: {}", tableMapping.getOracleFullTableName());
                return false;
            }
        }
        return true;
    }

    public void flushAll() {
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableMapping tableMapping = entry.getValue();

            for (TableRecordBuilder tableRecordBuilder : tableMapping.getTableRecordBuilders()) {
                tableRecordBuilder.sync();
            }
            tableMapping.getTopicWriter().sync();
        }
    }

    private RecordBuilder(Configure configure) {
        this.configure = configure;
        initDataHub();
        createBuilderAndWriter();
    }

    private void initDataHub() {
        HttpConfig httpConfig = new HttpConfig();
        if (StringUtils.isNotBlank(configure.getCompressType())) {
            boolean needCompress = false;
            for (HttpConfig.CompressType type : HttpConfig.CompressType.values()) {
                if (type.name().equals(configure.getCompressType())) {
                    httpConfig.setCompressType(HttpConfig.CompressType.valueOf(configure.getCompressType()));
                    needCompress = true;
                    break;
                }
            }
            if (!needCompress) {
                logger.warn("Invalid DataHub compressType, deploy no compress, compressType: {}",
                        configure.getCompressType());
            }
        }

        client = DatahubClientBuilder.newBuilder()
                .setUserAgent(Constant.PLUGIN_VERSION)
                .setDatahubConfig(
                        new DatahubConfig(configure.getDatahubEndpoint(),
                                new AliyunAccount(configure.getDatahubAccessId(), configure.getDatahubAccessKey()),
                                configure.isEnablePb()))
                .setHttpConfig(httpConfig)
                .build();

        for (TableMapping tableMapping : configure.getTableMappings().values()) {
            GetTopicResult topic = client.getTopic(tableMapping.getProjectName(), tableMapping.getTopicName());
            if (topic.getRecordType() == RecordType.TUPLE) {
                tableMapping.setRecordSchema(topic.getRecordSchema());
                checkTableMapping(tableMapping);
            }
        }
    }

    private void createBuilderAndWriter() {
        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {

            TableMapping tableMapping = entry.getValue();
            TopicWriter topicWriter = new TopicWriter(configure, entry.getValue(), client);
            tableMapping.setTopicWriter(topicWriter);

            List<TableRecordBuilder> tableRecordBuilders = new ArrayList<>();
            for (int i = 0; i < tableMapping.getBuildSpeed(); i++) {
                TableRecordBuilder tableRecordBuilder = new TableRecordBuilder(configure, topicWriter, entry.getValue(), i);
                tableRecordBuilders.add(tableRecordBuilder);
            }
            tableMapping.setTableRecordBuilders(tableRecordBuilders);
        }
    }

    private void checkTableMapping(TableMapping tableMapping) {
        if (tableMapping.getRecordSchema() == null) {
            return;
        }


        checkColumn(tableMapping.getRowIdColumn(), Arrays.asList(FieldType.STRING),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcTypeColumn(), Arrays.asList(FieldType.STRING),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcTimeColumn(), Arrays.asList(FieldType.STRING,
                FieldType.TIMESTAMP), tableMapping.getRecordSchema(), tableMapping.getTopicName());

        checkColumn(tableMapping.getcIdColumn(), Arrays.asList(FieldType.STRING, FieldType.BIGINT),
                tableMapping.getRecordSchema(), tableMapping.getTopicName());

        for (Map.Entry<String, String> entry : tableMapping.getConstColumnMappings().entrySet()) {
            checkColumn(entry.getKey(), Arrays.asList(FieldType.STRING),
                    tableMapping.getRecordSchema(), tableMapping.getTopicName());
        }

        for (Map.Entry<String, ColumnMapping> entry : tableMapping.getColumnMappings().entrySet()) {
            ColumnMapping columnMapping = entry.getValue();

            checkColumn(columnMapping.getDest(), null, tableMapping.getRecordSchema(), tableMapping.getTopicName());
            checkColumn(columnMapping.getDestOld(), null, tableMapping.getRecordSchema(), tableMapping.getTopicName());
        }
    }

    private void checkColumn(String columnName, List<FieldType> types, RecordSchema recordSchema, String topicName) {
        if (StringUtils.isBlank(columnName)) {
            return;
        }

        Field field = recordSchema.getField(columnName);
        if (field == null) {
            logger.error("CheckSchema failed, the field is not exist in DataHub, topic: {}, fieldName: {}", topicName, columnName);
            throw new IllegalArgumentException("the field is not exist in DataHub");
        }

        if (types != null) {
            boolean flag = false;
            for (FieldType type : types) {
                if (type == field.getType()) {
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                logger.error("CheckSchema failed, the oracle column corresponding field type is invalid in DataHub," +
                        " column: {}, filedType: {}", columnName, field.getType().name());
                throw new IllegalArgumentException("the oracle column corresponding field type is invalid in DataHub");
            }
        }
    }
}

