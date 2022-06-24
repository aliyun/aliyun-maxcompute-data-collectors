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

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.DatahubClientException;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.GetTopicResult;
import com.aliyun.datahub.client.model.ListShardResult;
import com.aliyun.datahub.client.model.RecordType;
import com.aliyun.datahub.client.model.ShardEntry;
import com.aliyun.datahub.client.model.ShardState;
import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ClientHelper {
    private final static Logger logger = LoggerFactory.getLogger(ClientHelper.class);
    private static final int LIST_SHARD_RETRY_TIMES = 30;
    private static final int LIST_SHARD_RETRY_INTERVAL_MS = 1000;

    private static ClientHelper clientHelper;

    private Configure configure;
    private DatahubClient client;

    private ClientHelper(Configure configure) {
        this.configure = configure;
    }

    public static void init(Configure configure) {
        if (clientHelper == null) {
            clientHelper = new ClientHelper(configure);
        }

        clientHelper.initClient();
    }

    // only for test
    private ClientHelper(DatahubClient client) {
        this.client = client;
    }

    public static void initForTest(DatahubClient client) {
        clientHelper = new ClientHelper(client);
    }

    private void initClient() {
        HttpConfig httpConfig = new HttpConfig();
        if (StringUtils.isNotBlank(configure.getCompressType())) {
            boolean needCompress = false;
            for (HttpConfig.CompressType type : HttpConfig.CompressType.values()) {
                if (type.name().equals(configure.getCompressType())) {
                    httpConfig.setCompressType(HttpConfig.CompressType.valueOf(configure.getCompressType()));
                    needCompress = true;
                    break;
                }
            }
            if (!needCompress) {
                logger.warn("Invalid DataHub compressType, deploy no compress, compressType: {}",
                        configure.getCompressType());
            }
        }

        client = DatahubClientBuilder.newBuilder()
                .setUserAgent(Constant.PLUGIN_VERSION)
                .setDatahubConfig(
                        new DatahubConfig(configure.getDatahubEndpoint(),
                                new AliyunAccount(configure.getDatahubAccessId(), configure.getDatahubAccessKey()),
                                configure.isEnablePb()))
                .setHttpConfig(httpConfig)
                .build();

        for (TableMapping tableMapping : configure.getTableMappings().values()) {
            GetTopicResult topic = client.getTopic(tableMapping.getProjectName(), tableMapping.getTopicName());
            if (topic.getRecordType() == RecordType.TUPLE) {
                tableMapping.setRecordSchema(topic.getRecordSchema());
            }
        }
    }

    public static ClientHelper instance() {
        return clientHelper;
    }

    public DatahubClient getClient() {
        return client;
    }

    private boolean isShardReady(List<ShardEntry> shardEntries) {
        for (ShardEntry shardEntry : shardEntries) {
            if (ShardState.CLOSING.equals(shardEntry.getState()) ||
                    ShardState.OPENING.equals(shardEntry.getState())) {
                return false;
            }
        }
        return true;
    }

    public List<String> getShardList(String projectName, String topicName) {
        int retryCount = 0;
        while (true) {
            try {
                List<ShardEntry> shardEntries = client.listShard(projectName, topicName).getShards();
                for (int i = 0; i < LIST_SHARD_RETRY_TIMES; ++i) {
                    if (isShardReady(shardEntries)) {
                        break;
                    }
                    if (i >= LIST_SHARD_RETRY_TIMES - 1) {
                        throw new DatahubClientException("Shard state not ready timeout");
                    }
                    logger.warn("Shard state not ready, will retry [{}], project: {}, topic: {}",
                            retryCount, projectName, topicName);
                    try {
                        Thread.sleep(LIST_SHARD_RETRY_INTERVAL_MS);
                    } catch (InterruptedException ignored) {
                    }

                }
                shardEntries.sort(Comparator.comparingInt(o -> Integer.parseInt(o.getShardId())));
                List<String> shardIds = new ArrayList<>();

                for (ShardEntry shardEntry : shardEntries) {
                    if (ShardState.ACTIVE.equals(shardEntry.getState())) {
                        shardIds.add(shardEntry.getShardId());
                    }
                }

                return shardIds;
            } catch (DatahubClientException e) {
                if (configure.getRetryTimes() > -1 && retryCount >= configure.getRetryTimes()) {
                    logger.error("List shard failed, project: {}, topic: {}", projectName, topicName, e);
                    throw e;
                } else {
                    logger.warn("List shard failed, will retry [{}], project: {}, topic: {}",
                            retryCount, projectName, topicName, e);
                    ++retryCount;
                }

                try {
                    Thread.sleep(configure.getRetryIntervalMs());
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
