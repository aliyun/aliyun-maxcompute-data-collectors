package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

public class ActionExecutorFactory {

  private static HiveSqlExecutor hiveSqlExecutor = null;
  private static OdpsExecutor odpsSqlExecutor = null;

  public static HiveSqlExecutor getHiveSqlExecutor() {
    if (hiveSqlExecutor == null) {
      hiveSqlExecutor = new HiveSqlExecutor();
    }

    return hiveSqlExecutor;
  }

  public static OdpsExecutor getOdpsSqlExecutor() {
    if (odpsSqlExecutor == null) {
      odpsSqlExecutor = new OdpsExecutor();
    }

    return odpsSqlExecutor;
  }

  public static void shutdown() {
    if (hiveSqlExecutor != null) {
      hiveSqlExecutor.shutdown();
    }

    if (odpsSqlExecutor != null) {
      odpsSqlExecutor.shutdown();
    }
  }
}
