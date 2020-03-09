package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;
import java.util.SortedSet;

public interface TaskManager {

  List<Task> generateTasks(SortedSet<Action> actions);

}
