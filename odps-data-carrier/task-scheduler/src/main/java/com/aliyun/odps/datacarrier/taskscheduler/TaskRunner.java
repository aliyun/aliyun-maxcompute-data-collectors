package com.aliyun.odps.datacarrier.taskscheduler;

public interface TaskRunner {

  void submitExecutionTask(Task task, Action action, String executionTaskName);

  void shutdown();
}
