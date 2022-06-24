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

import com.aliyun.datahub.client.model.*;
import com.aliyun.odps.ogg.handler.datahub.modle.ColumnMapping;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import oracle.goldengate.datasource.adapt.Op;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class RecordBuilder {
    private final static Logger logger = LoggerFactory.getLogger(RecordBuilder.class);
    private static RecordBuilder recordBuilder;

    private Configure configure;
    private ExecutorService executor;

    private Map<String, TableRecordBuilder> tableRecordBuilderMap;

    private RecordBuilder(Configure configure) {
        this.configure = configure;

        int corePoolSize = configure.getBuildRecordCorePoolSize() == -1
                ? configure.getTableMappings().size()
                : configure.getBuildRecordCorePoolSize();
        corePoolSize = Math.max(corePoolSize, 1);
        int maximumPoolSize = configure.getBuildRecordMaximumPoolSize() == -1
                ? corePoolSize * 2
                : configure.getBuildRecordMaximumPoolSize();
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L,
                TimeUnit.SECONDS, new SynchronousQueue<>(), new RecordBuilderThreadFactory());

        createBuilders();
    }

    public static RecordBuilder instance() {
        return recordBuilder;
    }

    public static void init(Configure configure) {
        if (recordBuilder == null) {
            recordBuilder = new RecordBuilder(configure);
        }

        recordBuilder.checkTableSchema();
        recordBuilder.start();
    }

    public void start() {
        for (TableRecordBuilder builder : tableRecordBuilderMap.values()) {
            builder.start();
        }
    }

    public void stop() {
        for (TableRecordBuilder builder : tableRecordBuilderMap.values()) {
            builder.stop();
        }
        executor.shutdown();
    }

    public static void destroy() {
        if (recordBuilder != null) {
            recordBuilder.flushAll();
            recordBuilder.stop();
        }

        recordBuilder = null;
    }

    public void flushAll() {
        for (TableRecordBuilder builder : tableRecordBuilderMap.values()) {
            builder.syncExec();
        }
    }

    public boolean buildRecord(Op op, String opType, String recordId) {
        String oracleFullTableName = op.getTableName().getFullName().toLowerCase();
        TableRecordBuilder recordBuilder = tableRecordBuilderMap.get(oracleFullTableName);

        if (recordBuilder != null) {
            return recordBuilder.addRecord(op, opType, recordId);
        } else {
            logger.warn("oracle table: {} not config", oracleFullTableName);
        }
        return true;
    }

    private void createBuilders() {
        tableRecordBuilderMap = new HashMap<>(configure.getTableMappings().size());

        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TableRecordBuilder recordBuilder = new TableRecordBuilder(configure, entry.getValue(), executor);
            tableRecordBuilderMap.put(entry.getKey(), recordBuilder);
        }
    }

    private void checkTableSchema() {
        for (TableMapping tableMapping : configure.getTableMappings().values()) {
            checkTableMapping(tableMapping);
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
            logger.error("CheckSchema failed, the field is not exist in DataHub, topic: {}, field: {}", topicName, columnName);
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

    private static class RecordBuilderThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        RecordBuilderThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "DataHub-builder-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }

            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}

