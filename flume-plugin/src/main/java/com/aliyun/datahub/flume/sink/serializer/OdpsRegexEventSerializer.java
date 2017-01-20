/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.datahub.flume.sink.serializer;

import com.aliyun.odps.Table;
import com.aliyun.odps.flume.sink.OdpsWriter;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OdpsRegexEventSerializer implements OdpsEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(OdpsRegexEventSerializer.class);

    public static final String ALIAS = "REGEX";

    // Config vars
    /** Regular expression used to parse groups from event data. */
    public static final String REGEX_CONFIG = "regex";
    public static final String REGEX_DEFAULT = "(.*)";

    /** Whether to ignore case when performing regex matches. */
    public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
    public static final boolean INGORE_CASE_DEFAULT = false;

    /** Comma separated list of column names to place matching groups in. */
    public static final String FIELD_NAMES = "fieldnames";
    public static final String FIELD_NAME_DEFAULT = "payload";

    /** What charset to use when serializing into HBase's byte arrays */
    public static final String CHARSET_CONFIG = "charset";
    public static final String CHARSET_DEFAULT = "UTF-8";

    private byte[] payload;
    private String[] inputColNames;
    private boolean regexIgnoreCase;
    private Pattern inputPattern;
    private Charset charset;

    @Override
    public void configure(Context context) {
        String regex = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
        regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,
            INGORE_CASE_DEFAULT);
        inputPattern = Pattern.compile(regex, Pattern.DOTALL
            + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
        charset = Charset.forName(context.getString(CHARSET_CONFIG,
            CHARSET_DEFAULT));

        String colNameStr = context.getString(FIELD_NAMES, FIELD_NAME_DEFAULT);
        inputColNames = colNameStr.split(",");
    }

    @Override public void initialize(Event event) {
        this.payload = event.getBody();
    }

    @Override public Map<String, String> getRow() throws UnsupportedEncodingException {
        Map<String, String> rowMap = Maps.newHashMap();
        Matcher m = inputPattern.matcher(new String(payload, charset));
        if (!m.matches()) {
            logger.debug("line not match regex!");
            return Maps.newHashMap();
        }

        if (m.groupCount() != inputColNames.length) {
            logger.debug("regex group num not match column num!");
            return Maps.newHashMap();
        }

        for (int i = 0; i < inputColNames.length; i++) {
            if (!StringUtils.isEmpty(inputColNames[i])) {
                rowMap.put(inputColNames[i], m.group(i + 1));
            }
        }

        return rowMap;
    }

    @Override public OdpsWriter createOdpsWriter(Table odpsTable, StreamWriter[] streamWriters,
        String dateFormat) throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Not implemented!");
    }

    @Override public String[] getInputColumnNames() {
        return inputColNames;
    }
}
