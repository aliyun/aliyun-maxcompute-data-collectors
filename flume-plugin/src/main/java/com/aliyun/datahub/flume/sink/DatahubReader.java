package com.aliyun.datahub.flume.sink;

import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.clientlibrary.config.ConsumerConfig;
import com.aliyun.datahub.clientlibrary.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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

    public void close() {
        consumer.close();
    }

    public RecordEntry read() {
        return consumer.read(maxRetry);
    }
}

