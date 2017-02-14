/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.datahub.flume.sink;

/**
 * Constants used for configuration of DatahubSink
 */
public class DatahubConfigConstants {

    private DatahubConfigConstants() {
    }

    /**
     * Maxcompute account accessID
     */
    public static final String MAXCOMPUTE_ACCESS_ID = "maxcompute.accessID";
    /**
     * Maxcompute account accessKey
     */
    public static final String MAXCOMPUTE_ACCESS_KEY = "maxcompute.accessKey";
    /**
     * Maxcompute endpoint
     */
    public static final String MAXCOMPUTE_END_POINT = "maxcompute.endPoint";
    /**
     * Maxcompute project name
     */
    public static final String MAXCOMPUTE_PROJECT = "maxcompute.project";
    /**
     * Maxcompute table name
     */
    public static final String MAXCOMPUTE_TABLE = "maxcompute.table";
    /**
     * Datahub account accessID
     */
    public static final String DATAHUB_ACCESS_ID = "datahub.accessID";
    /**
     * Datahub account accessKey
     */
    public static final String DATAHUB_ACCESS_KEY = "datahub.accessKey";
    /**
     * Datahub endpoint
     */
    public static final String DATAHUB_END_POINT = "datahub.endPoint";
    /**
     * Datahub project name
     */
    public static final String DATAHUB_PROJECT = "datahub.project";
    /**
     * Datahub topic name
     */
    public static final String DATAHUB_TOPIC = "datahub.topic";
    /**
     * Datahub shard id, optional
     */
    public static final String DATAHUB_SHARD_ID = "datahub.shard.id";
    /**
     * Datahub shard count
     */
    public static final String DATAHUB_TOPIC_SHARDCOUNT = "datahub.topic.shardcount";

    public static final String SHARD_COLUMNS = "datahub.shard.columns";
    public static final String DATE_FORMAT_COLUMNS = "dateformat.columns";

    public static final String MAXCOMPUTE_PARTITION_COLUMNS = "maxcompute.partition.columns";
    public static final String MAXCOMPUTE_PARTITION_VALUES = "maxcompute.partition.values";


    public static final String DATE_FORMAT = "dateFormat";
    public static final String BATCH_SIZE = "batchSize";
    public static final String SERIALIZER = "serializer";
    public static final String SERIALIZER_PREFIX = SERIALIZER + ".";

    public static final String RETRY_TIMES = "retryTimes";
    public static final String RETRY_INTERVAL = "retryInterval";

    public static final String USE_LOCAL_TIME_STAMP = "useLocalTimeStamp";
    public static final String TIME_ZONE = "timeZone";
    public static final String NEED_ROUNDING = "round";
    public static final String ROUND_UNIT = "roundUnit";
    public static final String HOUR = "hour";
    public static final String MINUTE = "minute";
    public static final String SECOND = "second";
    public static final String ROUND_VALUE = "roundValue";

    public static final String IS_BLANK_VALUE_AS_NULL = "isBlankValueAsNull";
}
