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
      case ODPS_VALIDATE:
        return RunnerType.ODPS;
      case HIVE_LOAD_DATA:
      case HIVE_VALIDATE:
        return RunnerType.HIVE;
      case VALIDATE:
        return RunnerType.VALIDATOR;
      case UNKNOWN:
      default:
        throw new RuntimeException("Unknown action: " + action.name());
    }
  }
}
