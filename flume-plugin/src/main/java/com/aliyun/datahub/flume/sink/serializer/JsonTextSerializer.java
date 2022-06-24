/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.datahub.flume.sink.serializer;

import com.aliyun.datahub.client.model.Field;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A simple delimited serializer that handles json textual events. It returns the field name to value map from an
 * event.
 */
public class JsonTextSerializer implements EventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(JsonTextSerializer.class);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static final String ALIAS = "JSON";

    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String CHARSET = "charset";
    public static final String FIELD_NAMES = "fieldnames";

    private String charset;
    private String[] inputColNames;
    private byte[] payLoad;

    @Override
    public void configure(Context context) {
        charset = context.getString(CHARSET, DEFAULT_CHARSET);
        String fieldNames = context.getString(FIELD_NAMES);
        if (fieldNames != null){
            inputColNames = fieldNames.split(",", -1);
        }

    }

    @Override
    public void initialize(Event event) {
        this.payLoad = event.getBody();
    }

    @Override
    public Map<String, String> getRow() throws UnsupportedEncodingException {
        Map<String, String> rowMap = Maps.newHashMap();
        String row = getRawBody();
        try {
            JsonNode tree = objectMapper.readTree(row);
            for (int i = 0; i < inputColNames.length; i++) {
                if (!StringUtils.isEmpty(inputColNames[i])) {
                    JsonNode node = tree.get(inputColNames[i]);
                    if (node != null) {
                        rowMap.put(inputColNames[i], (node.isObject() || node.isArray()) ? node.toString() : node.asText());
                    }
                }
            }
        } catch (IOException e) {
            logger.error("parse data {} to json error: {}", row, e);
        }
        return rowMap;
    }

    @Override
    public String[] getInputColumnNames() {
        return inputColNames;
    }

    @Override
    public String getRawBody() throws UnsupportedEncodingException {
        return new String(payLoad, charset);
    }

    @Override
    public Event getEvent(RecordEntry entry) {
        TupleRecordData data = (TupleRecordData) entry.getRecordData();

        Map<String, Object> jsonMap = new HashMap<String, Object>();
        List<Field> fields = data.getRecordSchema().getFields();
        for (int i = 0; i < fields.size(); ++i) {
            jsonMap.put(fields.get(i).getName(), data.getField(fields.get(i).getName()));
        }

        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Timestamp", String.valueOf(entry.getSystemTime()));
        headers.put("Sequence", String.valueOf(entry.getSequence()));
        headers.put("ShardId", entry.getShardId());
        headers.put("Cursor", entry.getCursor());
        if (entry.getAttributes() != null) {
            headers.putAll(entry.getAttributes());
        }

        try {
            return EventBuilder.withBody(objectMapper.writeValueAsString(jsonMap), Charset.forName(charset), headers);
        } catch (JsonProcessingException e) {
            logger.error("format json error: {}", e);
        }
        return null;
    }
}
