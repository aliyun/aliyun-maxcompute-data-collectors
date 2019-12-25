package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;
import java.util.SortedSet;

public interface TaskManager {

  TaskRunner getTaskRunner(RunnerType runnerType);

  List<Task> generateTasks(SortedSet<Action> actions, Mode mode);

  void shutdown();
}
