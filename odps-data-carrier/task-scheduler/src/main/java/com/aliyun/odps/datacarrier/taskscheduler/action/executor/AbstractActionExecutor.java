package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AbstractActionExecutor implements ActionExecutor {

  private static final Logger LOG = LogManager.getLogger(AbstractActionExecutor.class);

  private static final int DEFAULT_CORE_POOL_SIZE = 20;
  private static final int DEFAULT_MAX_POOL_SIZE = 50;
  private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60;

  ThreadPoolExecutor executor;

  public AbstractActionExecutor() {

    ThreadFactory factory = new ThreadFactoryBuilder()
        .setNameFormat("ActionExecutor-" + " #%d")
        .setDaemon(true)
        .build();

    // TODO: configurable
    this.executor = new ThreadPoolExecutor(
        DEFAULT_CORE_POOL_SIZE,
        DEFAULT_MAX_POOL_SIZE,
        DEFAULT_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        factory,
        (r, executor) -> LOG.warn("Failed to submit task to ThreadPoolExecutor:" + executor));
  }

  @Override
  public void shutdown() {
    this.executor.shutdown();
  }
}
