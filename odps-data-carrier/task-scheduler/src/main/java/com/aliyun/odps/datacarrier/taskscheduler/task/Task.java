package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.action.Action;

public interface Task {

  TaskProgress getProgress();

  List<Action> getExecutableActions();

  String getId();

  void stop();
}
