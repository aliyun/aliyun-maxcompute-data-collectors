package com.aliyun.odps.datacarrier.taskscheduler;

public class ExternalTableConfig {
  private ExternalTableStorage storage;

  public ExternalTableConfig(ExternalTableStorage tableStorage) {
    this.storage = tableStorage;
  }

  public ExternalTableStorage getStorage() {
    return this.storage;
  }
}
