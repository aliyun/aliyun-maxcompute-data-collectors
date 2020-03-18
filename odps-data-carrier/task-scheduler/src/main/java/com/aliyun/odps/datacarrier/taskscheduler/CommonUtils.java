package com.aliyun.odps.datacarrier.taskscheduler;

// TODO: move to another util class
public class CommonUtils {

  public static RunnerType getRunnerTypeByAction(Action action) {
    switch (action) {
      case ODPS_CREATE_TABLE:
      case ODPS_ADD_PARTITION:
      case ODPS_CREATE_EXTERNAL_TABLE:
      case ODPS_ADD_EXTERNAL_TABLE_PARTITION:
      case ODPS_LOAD_DATA:
      case ODPS_VERIFICATION:
        return RunnerType.ODPS;
      case HIVE_LOAD_DATA:
      case HIVE_VERIFICATION:
        return RunnerType.HIVE;
      case VERIFICATION:
        return RunnerType.VERIFICATION;
      case UNKNOWN:
      default:
        throw new RuntimeException("Unknown action: " + action.name());
    }
  }
}
