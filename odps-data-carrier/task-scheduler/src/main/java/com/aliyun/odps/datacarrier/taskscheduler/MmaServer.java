package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.hadoop.hive.metastore.api.MetaException;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSourceFactory;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImpl;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;


public class MmaServer {

  private TaskScheduler taskScheduler;

  public MmaServer() throws MetaException, MmaException {
    MmaMetaManager mmaMetaManager = new MmaMetaManagerDbImpl(
        null,
        MetaSourceFactory.getMetaSource(),
        true);
    TaskProvider taskProvider = new TaskProvider(mmaMetaManager);
    taskScheduler = new TaskScheduler(taskProvider);
  }

  public void run(){
    taskScheduler.run();
  }

  public void shutdown() {
    taskScheduler.shutdown();
  }
}
