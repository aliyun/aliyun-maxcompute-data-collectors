package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Objects;

public class MmaJobSuccceedEvent extends BaseMmaEvent {

  private String databaseName;
  private String tableName;

  public MmaJobSuccceedEvent(String databaseName, String tableName) {
    this.databaseName = Objects.requireNonNull(databaseName);
    this.tableName = Objects.requireNonNull(tableName);
  }

  @Override
  public MmaEventType getType() {
    return MmaEventType.JOB_SUCCEEDED;
  }

  @Override
  public String toString() {
    return String.format("Job succeeded: %s.%s", databaseName, tableName);
  }
}
