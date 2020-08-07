package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

public class ActionExecutorFactory {

  private static HiveSqlExecutor hiveSqlExecutor = null;

  // execute both OdpsSqlAction and OdpsNoSqlAction
  private static OdpsExecutor odpsExecutor = null;

  public static HiveSqlExecutor getHiveSqlExecutor() {
    if (hiveSqlExecutor == null) {
      hiveSqlExecutor = new HiveSqlExecutor();
    }

    return hiveSqlExecutor;
  }

  public static OdpsExecutor getOdpsExecutor() {
    if (odpsExecutor == null) {
      odpsExecutor = new OdpsExecutor();
    }

    return odpsExecutor;
  }

  public static void shutdown() {
    if (hiveSqlExecutor != null) {
      hiveSqlExecutor.shutdown();
    }

    if (odpsExecutor != null) {
      odpsExecutor.shutdown();
    }
  }
}
