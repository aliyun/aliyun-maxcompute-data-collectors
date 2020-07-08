package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.UUID;

public abstract class BaseMmaEvent {

  private String id;

  public BaseMmaEvent() {
    id = UUID.randomUUID().toString();
  }

  public String getId() {
    return id;
  }

  public abstract MmaEventType getType();
}
