/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.flume.sink;

/**
 * Constants used for configuration of OdpsSink
 */
public class ConfigConstants {

    private ConfigConstants() {
    }
    /**
     * Aliyun account accessID
     */
    public static final String ACCESS_ID = "accessID";
    /**
     * Aliyun account accessKey
     */
    public static final String ACCESS_KEY = "accessKey";
    /**
     * ODPS endpoint
     */
    public static final String ODPS_END_POINT = "odps.endPoint";
    /**
     * Datahub upload endpoint
     */
    public static final String ODPS_DATAHUB_END_POINT = "odps.datahub.endPoint";
    /**
     * ODPS project name
     */
    public static final String ODPS_PROJECT = "odps.project";
    /**
     * ODPS table name
     */
    public static final String ODPS_TABLE = "odps.table";
    public static final String ODPS_PARTITION = "odps.partition";
    public static final String AUTO_CREATE_PARTITION = "autoCreatePartition";
    public static final String DATE_FORMAT = "odps.dateFormat";
    public static final String BATCH_SIZE = "batchSize";
    public static final String SERIALIZER = "serializer";
    public static final String SERIALIZER_PREFIX = SERIALIZER + ".";
    public static final String SHARD_NUMBER = "shard.number";
    /**
     * unit in second
     */
    public static final String MAX_LOAD_SHARD_TIME = "shard.maxTimeOut";

    public static final String USE_LOCAL_TIME_STAMP = "useLocalTimeStamp";
    public static final String TIME_ZONE = "timeZone";
    public static final String NEED_ROUNDING = "round";
    public static final String ROUND_UNIT = "roundUnit";
    public static final String HOUR = "hour";
    public static final String MINUTE = "minute";
    public static final String SECOND = "second";
    public static final String ROUND_VALUE = "roundValue";

}
