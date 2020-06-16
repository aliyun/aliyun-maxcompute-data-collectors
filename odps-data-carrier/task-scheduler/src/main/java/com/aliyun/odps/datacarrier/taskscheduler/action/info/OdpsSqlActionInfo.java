package com.aliyun.odps.datacarrier.taskscheduler.action.info;

import java.util.List;

import com.aliyun.odps.utils.StringUtils;

public class OdpsSqlActionInfo extends AbstractActionInfo {
  private String instanceId;
  private String logView;
  private List<List<String>> result;

  public synchronized String getInstanceId() {
    return instanceId;
  }

  public synchronized String getLogView() {
    return logView;
  }

  public synchronized void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public synchronized void setLogView(String logView) {
    this.logView = logView;
  }

  public synchronized void setResult(List<List<String>> result) {
    this.result = result;
  }

  public synchronized List<List<String>> getResult() {
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    sb.append(this.getClass().getSimpleName());
    String instanceId = getInstanceId();
    if (!StringUtils.isNullOrEmpty(instanceId)) {
      sb.append(" ").append(instanceId);
    }
    sb.append("]");

    return sb.toString();
  }
}
