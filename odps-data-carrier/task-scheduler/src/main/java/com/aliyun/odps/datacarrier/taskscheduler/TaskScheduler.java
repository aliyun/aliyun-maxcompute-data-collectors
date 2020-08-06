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


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.executor.ActionExecutorFactory;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProgress;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;

public class TaskScheduler {

  private static final Logger LOG = LogManager.getLogger(TaskScheduler.class);

  private static final int GET_PENDING_TASK_INTERVAL_MS = 8000;
  private static final int DEFAULT_SCHEDULING_INTERVAL_MS = 4000;
  private static final int DEFAULT_FINISHED_ACTION_HANDLING_INTERVAL_MS = 2000;
  private static final int DROP_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int DROP_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT = 10;

  private volatile boolean keepRunning;

  private TaskProvider taskProvider;

  private final SchedulingThread schedulingThread;
  private final FinishedActionHandlingThread finishedActionHandlingThread;

  private final List<Task> tasks;
  private final List<Action> executingActions;

  public TaskScheduler(TaskProvider taskProvider) {
    this.taskProvider = Objects.requireNonNull(taskProvider);

    this.keepRunning = true;
    this.tasks = Collections.synchronizedList(new LinkedList<>());
    this.executingActions = Collections.synchronizedList(new LinkedList<>());

    this.schedulingThread = new SchedulingThread();
    this.schedulingThread.start();
    this.finishedActionHandlingThread = new FinishedActionHandlingThread();
    this.finishedActionHandlingThread.start();
  }

  public void run() {

    while (keepRunning) {
      List<Task> tasksToRemove = new LinkedList<>();
      synchronized (tasks) {
        for (Task task : tasks) {
          if (TaskProgress.SUCCEEDED.equals(task.getProgress())
              || TaskProgress.FAILED.equals(task.getProgress())) {
            tasksToRemove.add(task);
          }
        }
      }

      for (Task task : tasksToRemove) {
        LOG.info("Remove terminated task: {}, progress: {}",
                 task.getId(),
                 task.getProgress());
        tasks.remove(task);
      }

      try {
        List<Task> pendingTasks = taskProvider.get();
        LOG.info("New tasks: {}", pendingTasks);
        tasks.addAll(pendingTasks);

        LOG.info("Current tasks: {}", tasks);

        try {
          Thread.sleep(GET_PENDING_TASK_INTERVAL_MS);
        } catch (InterruptedException e) {
          LOG.warn("Main thread interrupted");
        }
      } catch (Exception e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
      }
    }
    shutdown();
  }

  private class SchedulingThread extends Thread {

    private int schedulingInterval = DEFAULT_SCHEDULING_INTERVAL_MS;

    SchedulingThread() {
      super("Scheduler");
    }

    @Override
    public void run() {
      LOG.info("Scheduling thread starts");
      while (keepRunning) {
        try {
          synchronized (tasks) {
            for (Task task : tasks) {
              List<Action> executableActions = task.getExecutableActions();
              for (Action action : executableActions) {
                // TODO: do we need beforeExecution ?
                // TODO: allocate resource
                // TODO: fatal errors -> stop the scheduler; other errors -> handlers
                action.execute();
                executingActions.add(action);
              }
            }
          }
        } catch (Throwable ex) {
          LOG.error("Exception on heartbeat", ex);
          ex.printStackTrace();
          // interrupt handler thread in case it waiting on the queue
          break;
        }

        try {
          Thread.sleep(schedulingInterval);
        } catch (InterruptedException e) {
          LOG.warn("Scheduling thread interrupted");
        }
      }

      LOG.info("Heartbeat thread ends");
    }
  }

  private class FinishedActionHandlingThread extends Thread {

    private int finishedActionHandlingInterval = DEFAULT_FINISHED_ACTION_HANDLING_INTERVAL_MS;

    FinishedActionHandlingThread() {
      super("FinishedActionHandler");
    }

    @Override
    public void run() {
      List<Action> finishedActions = new LinkedList<>();
      while (keepRunning) {
        synchronized (executingActions) {
          for (Action action : executingActions) {
            if (action.executionFinished()) {
              try {
                LOG.info("Action {} execute finish", action.getId());
                action.afterExecution();
              } catch (MmaException e) {
                // TODO: fatal errors -> stop the scheduler; other errors -> handlers
                LOG.error("Exception in after execution", e);
              }
              finishedActions.add(action);
              // TODO: release resource
            }
          }
        }

        executingActions.removeAll(finishedActions);

        try {
          Thread.sleep(finishedActionHandlingInterval);
        } catch (InterruptedException e) {
          LOG.warn("Finished action handling thread interrupted");
        }
      }
    }
  }

  public Map<String, TaskProgress> summary() {
    Map<String, TaskProgress> ret = new LinkedHashMap<>();
    synchronized (tasks) {
      for (Task task : tasks) {
        ret.put(task.getId(), task.getProgress());
      }
    }

    return ret;
  }

  public void shutdown() {
    LOG.info("Shutdown task runners.");

    keepRunning = false;

    try {
      schedulingThread.join();
    } catch (InterruptedException ignore) {
    }

    try {
      finishedActionHandlingThread.join();
    } catch (InterruptedException ignore) {
    }

    // TODO: stop tasks
    ActionExecutorFactory.shutdown();
  }
}
