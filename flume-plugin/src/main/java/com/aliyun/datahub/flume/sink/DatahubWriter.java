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

import com.aliyun.datahub.DatahubConfiguration;
import com.aliyun.datahub.auth.AliyunAccount;
import com.aliyun.datahub.model.*;
import com.aliyun.datahub.wrapper.Project;
import com.aliyun.datahub.wrapper.Topic;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DatahubWriter {
    private static final Logger logger = LoggerFactory.getLogger(DatahubWriter.class);

    private List<RecordEntry> recordCache = new ArrayList<RecordEntry>();

    private Configure configure;
    private Topic topic;
    SinkCounter sinkCounter;
    RecordBuilder recordBuilder;

    public DatahubWriter(Configure configure, SinkCounter sinkCounter) {
        this.configure = configure;
        this.sinkCounter = sinkCounter;

        DatahubConfiguration datahubConfiguration = new DatahubConfiguration(
            new AliyunAccount(configure.getAccessId(), configure.getAccessKey()),
            configure.getEndPoint());
        datahubConfiguration.setUserAgent("datahub-flume-plugin-1.1.0");

        Project project = Project.Builder.build(configure.getProject(), datahubConfiguration);
        topic = project.getTopic(configure.getTopic());

        if (topic == null) {
            throw new RuntimeException("Can not find datahub topic[" + configure.getTopic() + "]");
        }

        if (topic.getShardCount() == 0) {
            throw new RuntimeException("Topic[" + topic.getTopicName() + "] has not active shard");
        }

        // Initial record builder
        recordBuilder = new RecordBuilder(configure, topic);
        logger.info("Init RecordBuilder success");
    }

    public void addRecord(Map<String, String> rowData) throws ParseException {
        recordCache.add(recordBuilder.buildRecord(rowData));
    }

    public int getRecordSize() {
        return recordCache.size();
    }

    public void writeToHub() {
        if (recordCache == null || recordCache.size() == 0) {
            return;
        }

        List<RecordEntry> recordEntries = recordCache;
        recordBuilder.initRecordShardIds(recordEntries);
        int retryCount = 0;
        while (true) {
            try {
                PutRecordsResult putRecordsResult = topic.putRecords(recordEntries);

                //write had fail records
                if (putRecordsResult.getFailedRecordCount() > 0) {
                    recordEntries = putRecordsResult.getFailedRecords();
                    // need retry
                    if (configure.getRetryTimes() < 0 || (configure.getRetryTimes() >= 0
                        && retryCount < configure.getRetryTimes())) {
                        // should update shards before retry
                        if (configure.getShardId() == null) {
                            recordBuilder.updateShardIds();
                            recordBuilder.initRecordShardIds(recordEntries);
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            "Write " + String.valueOf(recordCache.size()) + " record to topic["
                                + topic.getTopicName() + "] success");
                    }

                    this.recordCache.clear();
                    break;
                }
            } catch (Exception e) {
                logger.warn("Write record to topic[" + topic.getTopicName() + "] failed", e);
            }

            if (configure.getRetryTimes() >= 0 && retryCount >= configure.getRetryTimes()) {
                throw new RuntimeException(
                    "Write record to topic[" + topic.getTopicName() + "] failed");
            }

            logger.warn(
                "Write record to topic[" + topic.getTopicName() + "] failed, will retry after "
                    + configure.getRetryInterval() + "seconds");
            try {
                Thread.sleep(configure.getRetryInterval() * 1000);
            } catch (InterruptedException e1) {
                // Do nothing
            }

            retryCount++;
        }
    }

}
