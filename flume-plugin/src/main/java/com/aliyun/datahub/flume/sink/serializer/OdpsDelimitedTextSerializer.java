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

import com.aliyun.odps.Table;
import com.aliyun.odps.flume.sink.OdpsWriter;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * A simple delimited serializer that handles dilimited textual events. It returns the field name to value map from an
 * event.
 */
public class OdpsDelimitedTextSerializer implements OdpsEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(OdpsDelimitedTextSerializer.class);

    public static final String ALIAS = "DELIMITED";

    public static final String DEFAULT_DELIMITER = ",";
    public static final String DEFAULT_CHARSET = "UTF-8";

    public static final String CHARSET = "charset";
    public static final String DELIMITER = "delimiter";
    public static final String FIELD_NAMES = "fieldnames";

    private String delimiter;
    private String charset;

    private String[] inputColNames;

    private byte[] payLoad;

    @Override
    public void configure(Context context) {
        delimiter = parseDelimiterSpec(context.getString(DELIMITER, DEFAULT_DELIMITER));
        charset = context.getString(CHARSET, DEFAULT_CHARSET);
        String fieldNames = Preconditions.checkNotNull(context.getString(FIELD_NAMES), "Field names cannot be empty, " +
                "please specify in configuration file");
        inputColNames = fieldNames.split(",", -1);
    }

    // if delimiter is a double quoted like "\t", drop quotes
    private static String parseDelimiterSpec(String delimiter) {
        if (delimiter == null) {
            return null;
        }
        if (delimiter.charAt(0) == '"' && delimiter.charAt(delimiter.length() - 1) == '"') {
            return delimiter.substring(1, delimiter.length() - 1);
        }
        return delimiter;
    }

    @Override
    public void initialize(Event event) {
        this.payLoad = event.getBody();
    }

    @Override
    public Map<String, String> getRow() throws UnsupportedEncodingException {
        String[] fieldValues = (new String(payLoad, charset)).split(delimiter, -1);
        if (inputColNames.length != fieldValues.length) {
            throw new RuntimeException("Serializing events failed. Check the " +
                "configuration in serializer. The filednames count (" +
                inputColNames.length + ") must equals fieldvalues count ( " +
                fieldValues.length + ")");
        }
        Map<String, String> rowMap = Maps.newHashMap();
        for (int i = 0; i < inputColNames.length; i++) {
            if (!StringUtils.isEmpty(inputColNames[i])) {
                rowMap.put(inputColNames[i], fieldValues[i]);
            }
        }
        return rowMap;
    }

    @Override
    public OdpsWriter createOdpsWriter(Table odpsTable, StreamWriter[] streamWriters, String dateFormat) {
        return new OdpsWriter(odpsTable, streamWriters, dateFormat, inputColNames);
    }

    @Override
    public String[] getInputColumnNames() {
        return inputColNames;
    }
}
