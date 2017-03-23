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

package com.aliyun.datahub.flume.sink;

import maxcompute.data.collectors.common.datahub.*;
import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.common.data.RecordSchema;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.datahub.model.ShardEntry;
import com.aliyun.datahub.model.ShardState;
import com.aliyun.datahub.wrapper.Topic;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecordBuilder {
    private final static Logger logger = LoggerFactory.getLogger(RecordBuilder.class);

    private Configure configure;
    private Topic topic;
    private List<String> shardIds = Lists.newArrayList();

    private Map<String, Field> columnMappings = new HashMap<String, Field>();
    private Map<String, Boolean> columnShardMappings = new HashMap<String, Boolean>();
    private Map<String, Boolean> columnDateformatMappings = new HashMap<String, Boolean>();

    private SimpleDateFormat dateFormat;

    private int lastShardIndex = 0;

    public RecordBuilder(Configure configure, Topic topic) {
        this.configure = configure;
        this.topic = topic;
        RecordSchema recordSchema = topic.getRecordSchema();

        dateFormat = new SimpleDateFormat(configure.getDateFormat());

        for (Field field : recordSchema.getFields()) {
            columnMappings.put(field.getName(), field);
        }
        // check validity of input columns
        Set<String> inputColumns = new HashSet<String>();
        for (String col : configure.getInputColumnNames()) {
            if (!StringUtils.isBlank(col) && !columnMappings.containsKey(col)) {
                throw new RuntimeException("Input column: " + col + " not exists in datahub!");
            }
            inputColumns.add(col);
        }

        for (String col : configure.getShardColumnNames()) {
            if (!columnMappings.containsKey(col)) {
                throw new RuntimeException("Shard column: " + col + " not exists in datahub!");
            }
            if (!inputColumns.contains(col)) {
                throw new RuntimeException(
                    "Shard column: " + col + " not exists in input columns!");
            }
            columnShardMappings.put(col, Boolean.TRUE);
        }
        for (String col : configure.getDateformatColumnNames()) {
            if (!columnMappings.containsKey(col)) {
                throw new RuntimeException("Dateformat column: " + col + " not exists in datahub!");
            }
            if (!inputColumns.contains(col)) {
                throw new RuntimeException(
                    "Dateformat column: " + col + " not exists in input columns!");
            }
            columnDateformatMappings.put(col, Boolean.TRUE);
        }

        List<ShardEntry> shardEntries = topic.listShard();
        for (ShardEntry shardEntry : shardEntries) {
            if (configure.getShardId() != null) {
                if (configure.getShardId().equals(shardEntry.getShardId())) {
                    if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                        shardIds.add(shardEntry.getShardId());
                    }
                    break;
                }
            } else {
                if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                    shardIds.add(shardEntry.getShardId());
                }
            }
        }

        updateShardIds();
        if (shardIds.size() == 0) {
            throw new RuntimeException("Topic[" + topic.getTopicName() + "] has not active shard");
        }
    }

    public void updateShardIds() {
        shardIds.clear();
        List<ShardEntry> shardEntries = topic.listShard();
        for (ShardEntry shardEntry : shardEntries) {
            if (configure.getShardId() != null) {
                if (configure.getShardId().equals(shardEntry.getShardId())) {
                    if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                        shardIds.add(shardEntry.getShardId());
                    }
                    break;
                }
            } else {
                if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                    shardIds.add(shardEntry.getShardId());
                }
            }
        }
    }

    public void initRecordShardIds(List<RecordEntry> recordEntries) {
        for (RecordEntry recordEntry : recordEntries) {
            if (configure.getShardId() != null) {
                recordEntry.setShardId(configure.getShardId());
            } else if (columnShardMappings.size() > 0) {
                // do nothing, already set
            } else {
                lastShardIndex = lastShardIndex % shardIds.size();
                recordEntry.setShardId(shardIds.get(lastShardIndex));
                lastShardIndex++;
            }
        }
    }

    public RecordEntry buildRecord(Map<String, String> rowData) throws ParseException {
        RecordEntry recordEntry = new RecordEntry(topic.getRecordSchema());
        for (Map.Entry<String, String> mapEntry : rowData.entrySet()) {
            String fieldName = mapEntry.getKey();
            if (!columnMappings.containsKey(fieldName)) {
                throw new RuntimeException("field name: " + fieldName + " not existed in datahub!");
            }
            Field field = columnMappings.get(fieldName);
            RecordUtil.setFieldValue(recordEntry, field, false, mapEntry.getValue(),
                columnDateformatMappings.containsKey(fieldName), dateFormat, configure.isBlankValueAsNull());
        }

        if (columnShardMappings.size() > 0) {
            StringBuilder hashKey = new StringBuilder();
            for (String col : configure.getShardColumnNames()) {
                hashKey.append(rowData.get(col));
            }
            int hashCode = hashKey.hashCode() & Integer.MAX_VALUE;
            recordEntry.setShardId(shardIds.get(hashCode % shardIds.size()));
        }

        return recordEntry;
    }
}
