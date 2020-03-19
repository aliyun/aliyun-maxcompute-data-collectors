package com.aliyun.odps.datacarrier.taskscheduler;

public enum Action {
  ODPS_CREATE_TABLE(1),
  ODPS_CREATE_EXTERNAL_TABLE(1),
  ODPS_ADD_PARTITION(2),
  ODPS_ADD_EXTERNAL_TABLE_PARTITION(2),
  ODPS_LOAD_DATA(3),
  HIVE_LOAD_DATA(3),
  ODPS_VERIFICATION(4),
  HIVE_VERIFICATION(4),
  VERIFICATION(5),
  UNKNOWN(Integer.MAX_VALUE);

  int priority;
  Action(int priority) {
    this.priority = priority;
  }

  public int getPriority() {
    return priority;
  }
}
