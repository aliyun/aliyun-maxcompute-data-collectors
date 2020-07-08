package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.Config;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventSenderType;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventType;

public class MmaEventConfig implements Config {

  public static class MmaEventSenderConfig {
    private MmaEventSenderType type;
    private String webhookUrl;

    public MmaEventSenderConfig(MmaEventSenderType type) {
      this.type = Objects.requireNonNull(type);
    }

    public void setWebhookUrl(String webhookUrl) {
      this.webhookUrl = webhookUrl;
    }

    public MmaEventSenderType getType() {
      return type;
    }

    public String getWebhookUrl() {
      return webhookUrl;
    }
  }

  private List<MmaEventType> blacklist;
  private List<MmaEventType> whitelist;
  private List<MmaEventSenderConfig> eventSenderConfigs;

  public MmaEventConfig() {
    blacklist = new LinkedList<>();
    whitelist = new LinkedList<>();
    eventSenderConfigs = new LinkedList<>();
  }

  public void setBlacklist(List<MmaEventType> blacklist) {
    this.blacklist = blacklist;
  }

  public void setWhitelist(List<MmaEventType> whitelist) {
    this.whitelist = whitelist;
  }

  public void setEventSenderConfigs(List<MmaEventSenderConfig> eventSenderConfigs) {
    this.eventSenderConfigs = eventSenderConfigs;
  }

  public List<MmaEventType> getBlacklist() {
    return blacklist;
  }

  public List<MmaEventType> getWhitelist() {
    return whitelist;
  }

  public List<MmaEventSenderConfig> getEventSenderConfigs() {
    return eventSenderConfigs;
  }

  @Override
  public boolean validate() {
    return true;
  }
}
