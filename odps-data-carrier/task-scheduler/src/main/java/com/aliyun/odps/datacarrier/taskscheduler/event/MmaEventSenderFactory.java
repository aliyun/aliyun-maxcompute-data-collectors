package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Objects;

import com.aliyun.odps.datacarrier.taskscheduler.MmaEventConfig.MmaEventSenderConfig;

public class MmaEventSenderFactory {

  public static MmaEventSender get(MmaEventSenderConfig config) {
    Objects.requireNonNull(config);

    switch (config.getType()) {
      case DingTalk:
        return new MmaEventSenderDingTalkImpl(config.getWebhookUrl());
      default:
        throw new IllegalArgumentException("Unsupported event sender type: " + config.getType());
    }
  }
}
