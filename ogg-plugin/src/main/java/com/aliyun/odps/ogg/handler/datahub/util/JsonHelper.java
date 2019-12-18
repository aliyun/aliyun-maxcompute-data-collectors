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

package com.aliyun.odps.ogg.handler.datahub.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * @author liqiang
 * @date 15/4/17
 */
public class JsonHelper {
    private static final Logger LOG = LoggerFactory.getLogger(JsonHelper.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private JsonHelper() {
    }

    public static JsonNode getJsonNodeFromString(String jsonString) {
        try {
            return objectMapper.readTree(jsonString);
        } catch (IOException e) {
            LOG.error("", e);
            return null;
        }
    }

    public static String beanToJson(Object bean) {
        StringWriter sw = new StringWriter();
        try {
            JsonGenerator jsongenerator = objectMapper.getFactory().createGenerator(sw);
            objectMapper.writeValue(jsongenerator, bean);
            jsongenerator.close();
        } catch (IOException e) {
            LOG.error("", e);
            return "";
        }
        return sw.toString();

    }

    public static <T> T jsonToBean(String jsonString, Class<T> clazz) {

        if (Strings.isNullOrEmpty(jsonString)) {
            return null;
        }
        try {
            return objectMapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            LOG.error("", e);
            return null;
        }
    }

    /*public static <T> T jsonToBean(String jsonString, TypeReference valueTypeRef) {

        if (Strings.isNullOrEmpty(jsonString)) {
            return null;
        }
        try {
            return objectMapper.readValue(jsonString, valueTypeRef);
        } catch (IOException e) {
            LOG.error("", e);
            return null;
        }
    }*/

    public static <T> T jsonToBeanWithException(String jsonString, Class<T> clazz) {

        if (Strings.isNullOrEmpty(jsonString)) {
            return null;
        }
        try {
            return objectMapper.readValue(jsonString, clazz);
        } catch (IOException e) {
            LOG.error("", e);
            throw new RuntimeException("Deserialize failed", e);
        }
    }

}
