package com.aliyun.odps.datacarrier.taskscheduler.action.info;

import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;

public class VerificationActionInfo extends AbstractActionInfo {
  private List<List<String>> succeededPartitions = new LinkedList<>();
  private List<List<String>> failedPartitions = new LinkedList<>();

  public List<List<String>> getSucceededPartitions() {
    return succeededPartitions;
  }

  public List<List<String>> getFailedPartitions() {
    return failedPartitions;
  }

  public void setSucceededPartitions(List<List<String>> succeededPartitions) {
    this.succeededPartitions = succeededPartitions;
  }

  public void setFailedPartitions(List<List<String>> failedPartitions) {
    this.failedPartitions = failedPartitions;
  }

  @Override
  public String toString() {
    //TODO: to string
    return null;
  }
}
