package com.aliyun.odps.datacarrier.taskscheduler.action.info;

import java.util.List;
import java.util.Objects;

import com.aliyun.odps.utils.StringUtils;

public class HiveSqlActionInfo extends AbstractActionInfo {

  private String jobId;
  private String trackingUrl;
  private List<List<String>> result;

  public synchronized String getJobId() {
    return jobId;
  }

  public synchronized String getTrackingUrl() {
    return trackingUrl;
  }

  public synchronized List<List<String>> getResult() {
    return result;
  }

  public synchronized void setJobId(String jobId) {
    this.jobId = Objects.requireNonNull(jobId);
  }

  public synchronized void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = Objects.requireNonNull(trackingUrl);
  }

  public synchronized void setResult(List<List<String>> result) {
    this.result = result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(this.getClass().getSimpleName());
    String jobId = getJobId();
    if (!StringUtils.isNullOrEmpty(jobId)) {
      sb.append(" ").append(jobId);
    }
    sb.append("]");

    return sb.toString();
  }
}
