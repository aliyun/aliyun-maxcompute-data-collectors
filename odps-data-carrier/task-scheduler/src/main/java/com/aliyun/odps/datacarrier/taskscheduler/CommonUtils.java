package com.aliyun.odps.datacarrier.taskscheduler;

import static com.aliyun.odps.datacarrier.commons.IntermediateDataManager.*;

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
      case VALIDATION:
      case VALIDATION_BY_PARTITION:
        return RunnerType.LOCAL;
      case UNKNOWN:
      default:
        throw new RuntimeException("Unknown action: " + action.name());
    }
  }

  public static Action getSqlActionFromDir(String scriptDir) {
    if (ODPS_DDL_DIR.equals(scriptDir)) {
      return Action.ODPS_DDL;
    } else if (ODPS_EXTERNAL_DDL_DIR.equals(scriptDir)) {
      return Action.ODPS_EXTERNAL_DDL;
    } else if (HIVE_UDTF_DIR.equals(scriptDir)) {
      return Action.HIVE_LOAD_DATA;
    } else if (ODPS_OSS_TRANSFER_DIR.equals(scriptDir)) {
      return Action.ODPS_LOAD_DATA;
    } else if (HIVE_VERIFY_DIR.equals(scriptDir)) {
      return Action.HIVE_VALIDATE;
    } else if (ODPS_VERIFY_DIR.equals(scriptDir)) {
      return Action.ODPS_VALIDATE;
    }
    return Action.UNKNOWN;
  }

}
