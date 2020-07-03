package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProgress;

public class MmaSummaryEvent extends BaseMmaEvent {

  private int numPendingJobs;
  private int numRunningJobs;
  private int numFailedJobs;
  private int numSucceededJobs;
  private Map<String, TaskProgress> taskToProgress;

  public MmaSummaryEvent(
      int numPendingJobs,
      int numRunningJobs,
      int numFailedJobs,
      int numSucceededJobs,
      Map<String, TaskProgress> taskToProgress) {
    this.numPendingJobs = numPendingJobs;
    this.numRunningJobs = numRunningJobs;
    this.numFailedJobs = numFailedJobs;
    this.numSucceededJobs = numSucceededJobs;
    this.taskToProgress = taskToProgress;
  }

  @Override
  public MmaEventType getType() {
    return MmaEventType.SUMMARY;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Job Overview:\n");
    sb.append("> pending: ").append(numPendingJobs).append("\n\n");
    sb.append("> running: ").append(numRunningJobs).append("\n\n");
    sb.append("> failed: ").append(numFailedJobs).append("\n\n");
    sb.append("> succeeded: ").append(numSucceededJobs).append("\n\n");
    sb.append("Task Progress:\n");
    for (Entry<String, TaskProgress> entry : taskToProgress.entrySet()) {
      sb.append("> task id: ").append(entry.getKey())
        .append(", progress: ").append(entry.getValue().name()).append("\n\n");
    }
    return sb.toString();
  }
}
