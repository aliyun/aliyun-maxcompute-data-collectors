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
     * Datahub account accessID
     */
    public static final String DATAHUB_ACCESS_ID = "datahub.accessId";
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
     * Datahub shard ids, optional
     */
    public static final String DATAHUB_SHARD_IDS = "datahub.shard.ids";

    public static final String DATAHUB_SUB_ID = "datahub.subId";
    public static final String DATAHUB_START_TIME = "datahub.startTime";

    public static final String DATAHUB_ENABLE_PB = "datahub.enablePb";
    public static final String DATAHUB_COMPRESS_TYPE = "datahub.compressType";

    public static final String RETRY_TIMES = "datahub.retryTimes";
    public static final String RETRY_INTERVAL = "datahub.retryInterval";

    public static final String BATCH_SIZE = "datahub.batchSize";
    public static final String MAX_Buffer_SIZE = "datahub.maxBufferSize";
    public static final String BATCH_TIMEOUT = "datahub.batchTimeout";
    public static final String SERIALIZER = "serializer";
    public static final String SERIALIZER_PREFIX = SERIALIZER + ".";

    public static final String Dirty_DATA_CONTINUE = "datahub.dirtyDataContinue";
    public static final String Dirty_DATA_FILE = "datahub.dirtyDataFile";

    public static final String AUTO_COMMIT = "datahub.autoCommit";
    public static final String OFFSET_COMMIT_INTERVAL = "datahub.offsetCommitInterval";
    public static final String SESSION_TIMEOUT = "datahub.sessionTimeout";
}
