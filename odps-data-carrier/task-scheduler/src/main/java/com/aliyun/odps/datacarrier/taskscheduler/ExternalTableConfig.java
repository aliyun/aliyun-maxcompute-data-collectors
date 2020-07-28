package com.aliyun.odps.datacarrier.taskscheduler;

public class ExternalTableConfig {
  private ExternalTableStorage storage;
  private String location;

  public ExternalTableConfig(ExternalTableStorage tableStorage, String location) {
    this.storage = tableStorage;
    this.location = location;
  }

  public ExternalTableStorage getStorage() {
    return this.storage;
  }

  public String getLocation() {
    return location;
  }
}
