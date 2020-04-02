package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;

public class MmaExceptionFactory {

  public static MmaException getFailedToCreateConnectionException(Throwable e) {
    return new MmaException("Failed to create connection", e);
  }

  public static MmaException getRunningMigrationJobExistsException(String db, String tbl) {
    String errorMsg = String.format("Running migration job exists, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg);
  }

  public static MmaException getFailedToAddMigrationJobException(String db,
                                                                 String tbl,
                                                                 Throwable e) {
    String errorMsg = String.format("Failed to add migration job, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg, e);
  }

  public static MmaException getFailedToRemoveMigrationJobException(String db,
                                                                    String tbl,
                                                                    Throwable e) {
    String errorMsg = String.format("Failed to remove migration job, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg, e);
  }

  public static MmaException getFailedToUpdateMigrationJobException(String db,
                                                                    String tbl,
                                                                    Throwable e) {
    String errorMsg = String.format("Failed to update migration job, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg, e);
  }

  public static MmaException getFailedToGetMigrationJobException(String db,
                                                                 String tbl,
                                                                 Throwable e) {
    String errorMsg = String.format("Failed to get migration job, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg, e);
  }

  public static MmaException getFailedToGetMigrationJobException(String db,
                                                                 String tbl) {
    String errorMsg = String.format("Failed to get migration job, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg);
  }

  public static MmaException getFailedToGetMigrationJobPtException(String db,
                                                                   String tbl,
                                                                   List<String> partitionValues) {
    String errorMsg =
        String.format("Failed to get migration job, db: %s, tbl: %s, pt: %s",
                      db, tbl, partitionValues);
    return new MmaException(errorMsg);
  }

  public static MmaException getFailedToListMigrationJobsException(Throwable e) {
    return new MmaException("Failed to list migration jobs", e);
  }

  public static MmaException getFailedToGetPendingJobsException(Throwable e) {
    return new MmaException("Failed to get pending jobs", e);
  }

  public static MmaException getMigrationJobNotExistedException(String db, String tbl) {
    String errorMsg = String.format("Migration job not existed, db: %s, tbl: %s", db, tbl);
    return new MmaException(errorMsg);
  }

  public static MmaException getMigrationJobPtNotExistedException(String db,
                                                                  String tbl,
                                                                  List<String> partitionValues) {
    String errorMsg =
        String.format("Migration job partition not existed, db: %s, tbl: %s, pt: %s",
                      db, tbl, partitionValues);
    return new MmaException(errorMsg);
  }
}
