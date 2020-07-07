package com.aliyun.odps.datacarrier.taskscheduler.action.info;

import com.aliyun.odps.utils.StringUtils;

public class OdpsSqlActionInfo extends AbstractActionInfo {
  /**
   * the result of some query cannot be parsed as columns, such as SHOW CREATE TABLE xxx
   * in this case, raw data result should be return
   */
  public enum ResultType {
    COLUMNS,
    RAW_DATA
  }

  private String instanceId;
  private String logView;
  ResultType resultType = ResultType.COLUMNS;

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

  public synchronized void setResultType(ResultType type) {
    this.resultType = type;
  }

  public synchronized ResultType getResultType() {
    return this.resultType;
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
