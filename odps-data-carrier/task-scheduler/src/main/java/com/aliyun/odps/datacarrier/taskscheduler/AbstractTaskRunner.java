/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
