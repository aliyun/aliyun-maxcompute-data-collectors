package com.aliyun.odps.datacarrier.taskscheduler;

import java.nio.file.Path;
import java.util.Map;

abstract class AbstractExecutionInfo {
  protected Progress progress = Progress.NEW;
}
