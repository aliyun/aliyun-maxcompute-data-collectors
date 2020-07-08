package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Objects;

public class MmaJobFailedEvent extends BaseMmaEvent {
  // TODO: include reason and possible solution

  private String databaseName;
  private String tableName;

  public MmaJobFailedEvent(String databaseName, String tableName) {
    this.databaseName = Objects.requireNonNull(databaseName);
    this.tableName = Objects.requireNonNull(tableName);
  }

  @Override
  public MmaEventType getType() {
    return MmaEventType.JOB_FAILED;
  }

  @Override
  public String toString() {
    return String.format("Job failed: %s.%s", databaseName, tableName);
  }
}
