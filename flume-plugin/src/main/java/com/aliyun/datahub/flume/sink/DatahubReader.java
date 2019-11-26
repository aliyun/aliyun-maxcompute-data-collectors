package com.aliyun.datahub.flume.sink;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatahubReader {
    private static final Logger logger = LoggerFactory.getLogger(DatahubReader.class);
    private Consumer consumer;
    private Configure configure;
    private int maxRetry;

    public DatahubReader(Configure configure) {
        this.configure = configure;
        maxRetry = configure.getRetryTimes();

        init();
    }

    private void init() {
        handleOffset();

        ConsumerConfig config = new ConsumerConfig(configure.getEndPoint(), configure.getAccessId(), configure.getAccessKey());
        config.setFetchSize(configure.getBatchSize());
        config.setAutoCommit(configure.isAutoCommit());
        config.setOffsetCommitTimeoutMs(configure.getOffsetCommitInterval() * 1000);
        config.setSessionTimeoutMs(configure.getSessionTimeout() * 1000);
        config.getDatahubConfig().setEnableBinary(configure.isEnablePb());

        if (configure.getCompressType() != null) {
            config.getHttpConfig().setCompressType(HttpConfig.CompressType.valueOf(configure.getCompressType()));
        }

        List<String> shardIds = configure.getShardIds();
        if (shardIds != null && !shardIds.isEmpty()) {
            consumer = new Consumer(configure.getProject(), configure.getTopic(), configure.getSubId(), shardIds, config);
        } else {
            consumer = new Consumer(configure.getProject(), configure.getTopic(), configure.getSubId(), config);
        }
    }

    private void handleOffset() {
        long timestamp = configure.getStartTimestamp();
        if (timestamp < 0) {
            return;
        }
        DatahubClient client = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(configure.getEndPoint(),
                                new AliyunAccount(configure.getAccessId(), configure.getAccessKey()), true))
                .build();

        ListShardResult listShardResult = client.listShard(configure.getProject(), configure.getTopic());
        List<String> shardIds = new ArrayList<String>();
        for (ShardEntry entry : listShardResult.getShards()) {
            shardIds.add(entry.getShardId());
        }

        // compare startTime and sub current time.
        GetSubscriptionOffsetResult getSubscriptionOffsetResult =
                client.getSubscriptionOffset(configure.getProject(), configure.getTopic(), configure.getSubId(), shardIds);
        for (Map.Entry<String, SubscriptionOffset> entry : getSubscriptionOffsetResult.getOffsets().entrySet()) {
            long ts = entry.getValue().getTimestamp();
            if (ts < timestamp) {
                logger.warn("[shard {}] The startTime less than the current subscription time, may cause some data not to be consumed", entry.getKey());
            } else if (ts > timestamp) {
                logger.warn("[shard {}] The startTime greater than the current subscription time, may cause some data to be repeatedly consumed", entry.getKey());
            }
        }

        Map<String, SubscriptionOffset> offsetMap = new HashMap<String, SubscriptionOffset>();
        for (String shardId : shardIds) {
            SubscriptionOffset offset = new SubscriptionOffset();
            offset.setTimestamp(timestamp);
            long sequence = client.getCursor(configure.getProject(), configure.getTopic(), shardId, CursorType.SYSTEM_TIME, timestamp).getSequence();
            offset.setSequence(sequence - 1);
            offsetMap.put(shardId, offset);
        }
        client.resetSubscriptionOffset(configure.getProject(), configure.getTopic(), configure.getSubId(), offsetMap);
        logger.info("reset subscription offset successful");
    }

    public void close() {
        consumer.close();
    }

    public RecordEntry read() {
        return consumer.read(maxRetry);
    }
}

