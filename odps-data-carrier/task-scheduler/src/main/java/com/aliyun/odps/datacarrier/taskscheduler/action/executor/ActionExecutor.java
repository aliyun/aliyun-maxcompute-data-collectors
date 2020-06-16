package com.aliyun.odps.datacarrier.taskscheduler.action.executor;


import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

public interface ActionExecutor {
  void shutdown() throws MmaException;
}
