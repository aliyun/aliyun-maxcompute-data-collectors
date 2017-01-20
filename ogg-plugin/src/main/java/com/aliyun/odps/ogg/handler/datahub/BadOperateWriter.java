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

import com.aliyun.datahub.common.data.Field;
import com.aliyun.datahub.model.RecordEntry;
import com.aliyun.odps.ogg.handler.datahub.modle.DirtyRecordInfo;
import com.aliyun.odps.ogg.handler.datahub.util.JsonHelper;
import com.beust.jcommander.internal.Maps;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by lyf0429 on 16/5/20.
 */
public class BadOperateWriter {
    private final static Logger logger = LoggerFactory.getLogger(BadOperateWriter.class);

    public static void checkFileSize(String fileName, int maxFileSize) {
        File file = new File(fileName);

        if (file.exists() && file.length() > maxFileSize) {
            String bakFileName = fileName + ".bak";
            File bakFile = new File(bakFileName);
            file.renameTo(new File(bakFile.getAbsolutePath()));
        }
    }

    public static void write(Op op, String oracleFullTableName, String topicName, String fileName,
        int maxFileSize, String msg) {
        checkFileSize(fileName, maxFileSize);

        DirtyRecordInfo dirtyRecordInfo = new DirtyRecordInfo();
        dirtyRecordInfo.setOracleTable(oracleFullTableName);
        dirtyRecordInfo.setTopicName(topicName);
        dirtyRecordInfo.setShardId(null);
        dirtyRecordInfo.setErrorMessage(msg);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dirtyRecordInfo.setErrorTime(simpleDateFormat.format(new Date()));

        Map<String, String> record = Maps.newHashMap();
        dirtyRecordInfo.setRecord(record);

        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            record.put(colName, cols.get(i).getAfterValue());
        }
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
            bw.write(JsonHelper.beanToJson(dirtyRecordInfo) + "\n");
            bw.close();
        } catch (IOException e) {
            logger.error("logBadOperation() failed. ", e);
            throw new RuntimeException("logBadOperation() failed. ", e);
        }
    }

    public static void write(RecordEntry recordEntry, String oracleFullTableName, String topicName,
        String fileName, int maxFileSize, String msg) {
        checkFileSize(fileName, maxFileSize);

        DirtyRecordInfo dirtyRecordInfo = new DirtyRecordInfo();
        dirtyRecordInfo.setOracleTable(oracleFullTableName);
        dirtyRecordInfo.setTopicName(topicName);
        dirtyRecordInfo.setShardId(recordEntry.getShardId());
        dirtyRecordInfo.setErrorMessage(msg);

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dirtyRecordInfo.setErrorTime(simpleDateFormat.format(new Date()));

        Map<String, String> record = Maps.newHashMap();
        dirtyRecordInfo.setRecord(record);

        Field[] fields = recordEntry.getFields();
        JsonNode jsonNode = recordEntry.toJsonNode();
        ArrayNode arrayNode = (ArrayNode) jsonNode.get("Data");

        for (int i = 0; i < recordEntry.getFieldCount(); i++) {
            record.put(fields[i].getName(), arrayNode.get(i).getTextValue());
        }

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
            bw.write(JsonHelper.beanToJson(dirtyRecordInfo) + "\n");
            bw.close();
        } catch (IOException e) {
            logger.error("logBadOperation() failed. ", e);
            throw new RuntimeException("logBadOperation() failed. ", e);
        }
    }
}
