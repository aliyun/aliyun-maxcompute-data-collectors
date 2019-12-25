package com.aliyun.odps.datacarrier.taskscheduler;

import java.nio.file.Path;

public class OdpsExecutionInfo extends AbstractExecutionInfo {

  private String instanceId;
  private String logView;

  public OdpsExecutionInfo(Path sqlPath) {
    super(sqlPath);
  }

  public OdpsExecutionInfo(String sqlStatement) {
    super(sqlStatement);
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public void setLogView(String logView) {
    this.logView = logView;
  }

  public String getOdpsExecutionInfoSummary () {
    final StringBuilder sb = new StringBuilder("OdpsExecutionInfo: ");
    if (isScriptMode()) {
      sb.append("SqlPath=").append(getSqlPath());
    } else {
      sb.append("Sql=").append(getSqlStatements());
    }
    sb.append("\nInstanceId=").append(instanceId);
    sb.append("\nLogView=").append(logView);
    sb.append("\n");
    return sb.toString();
  }
}
