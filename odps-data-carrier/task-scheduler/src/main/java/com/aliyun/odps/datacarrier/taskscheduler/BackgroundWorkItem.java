package com.aliyun.odps.datacarrier.taskscheduler;

public abstract class BackgroundWorkItem {
  public abstract void execute();
  public abstract boolean finished();
}
