package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;

public class HiveActionInfo extends AbstractActionInfo {

  private String jobId;
  private String trackingUrl;
  private List<List<String>> result;

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  public void setResult(List<List<String>> result) {
    this.result = result;
  }

  public List<List<String>> getResult() {
    return result;
  }

  public String getHiveActionInfoSummary() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\nJobId=").append(jobId);
    sb.append("\nTrackingUrl=").append(trackingUrl);
    sb.append("\n");
    return sb.toString();
  }
}
