package com.aliyun.odps.datacarrier.taskscheduler;

public interface TaskRunner {

  void submitExecutionTask(Task task, Action action) throws MmaException;

  void shutdown();
}
