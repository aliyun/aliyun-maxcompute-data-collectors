package com.aliyun.odps.datacarrier.taskscheduler;

import java.nio.file.Path;

public class HiveExecutionInfo extends AbstractExecutionInfo {

  private String jobId;
  private String trackingUrl;

  public HiveExecutionInfo(Path sqlPath) {
   super(sqlPath);
  }

  public HiveExecutionInfo(String sql) {
    super(sql);
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  public String getHiveExecutionInfoSummary () {
    final StringBuilder sb = new StringBuilder("HiveExecutionInfo: ");
    if (isScriptMode()) {
      sb.append("SqlPath=").append(getSqlPath());
    } else {
      sb.append(getSqlStatements());
      sb.append("Sql=").append(getSqlStatements());
    }
    sb.append("\nJobId=").append(jobId);
    sb.append("\nTrackingUrl=").append(trackingUrl);
    sb.append("\n");
    return sb.toString();
  }
}
