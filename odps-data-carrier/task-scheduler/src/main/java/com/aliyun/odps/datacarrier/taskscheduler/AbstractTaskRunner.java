package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

abstract class AbstractTaskRunner implements TaskRunner {

  private static final Logger LOG = LogManager.getLogger(AbstractTaskRunner.class);

  private static final int CORE_POOL_SIZE = 20;
  protected ThreadPoolExecutor runnerPool;

  public AbstractTaskRunner() {
    ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(
        "TaskRunner-" + " #%d").setDaemon(true).build();
    this.runnerPool = new ThreadPoolExecutor(
        CORE_POOL_SIZE,
        Integer.MAX_VALUE,
        1,
        TimeUnit.HOURS,
        new LinkedBlockingQueue<>(), factory,
        new RunnerRejectedExecutionHandler());
  }

  private static class RunnerRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      System.out.print("Can't submit task to ThreadPoolExecutor:" + executor);
    }
  }

  @Override
  public void shutdown() {
    runnerPool.shutdown();
  }
}
