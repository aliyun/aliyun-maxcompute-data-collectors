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


import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;

// TODO: move main to a new class MmaServerMain
public class TaskScheduler {

  private static final Logger LOG = LogManager.getLogger(TaskScheduler.class);

  private static final int GET_PENDING_TABLE_INTERVAL_MS = 20000;
  private static final int HEARTBEAT_INTERVAL_MS = 10000;
  private static final int DROP_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int DROP_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;
  private static final int VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT = Integer.MAX_VALUE;

  private Map<Action, ActionScheduleInfo> actionScheduleInfoMap;
  private SortedSet<Action> actions;
  protected Map<RunnerType, TaskRunner> taskRunnerMap;
  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected final List<Task> tasks;
  private MmaServerConfig mmaServerConfig;
  private MmaMetaManager mmaMetaManager;

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = Collections.synchronizedList(new LinkedList<>());
  }

  private void run(MmaServerConfig mmaServerConfig) throws TException, MmaException {

    // TODO: check if datasource and metasource are valid
    this.mmaServerConfig = mmaServerConfig;
    mmaMetaManager = new MmaMetaManagerDbImpl(null,
                                              CommonUtils.getMetaSource(mmaServerConfig),
                                              true);
    initActions(mmaServerConfig.getDataSource());
    initTaskRunner();
    updateConcurrencyThreshold();
    this.heartbeatThread.start();

    while (keepRunning) {
      List<Task> tasksToRemove = new LinkedList<>();
      synchronized (tasks) {
        InPlaceUpdates.resetScreen(System.out);
        for (Task task : tasks) {
          String detailedProgress =
              String.join("->",
                          task.actionInfoMap.entrySet().stream().map(
                              e -> "[" + e.getKey().name() + " " + e.getValue().progress
                                  .toColorString() + "]").collect(Collectors.toList()));
          System.out.println(task.getName() + " " + detailedProgress + "\n");

          if (Progress.SUCCEEDED.equals(task.getProgress())
              || Progress.FAILED.equals(task.getProgress())) {
            tasksToRemove.add(task);
          }
        }
      }

      for (Task task : tasksToRemove) {
        LOG.info("Remove terminated task: {}, progress: {}",
                 task.getName(),
                 task.getProgress());
        tasks.remove(task);
      }

      LOG.info("Looking for pending tables & partitions");
      try {
        List<MetaSource.TableMetaModel> pendingTables = mmaMetaManager.getPendingTables();

        if (!pendingTables.isEmpty()) {
          LOG.info("Tables to migrate");
          for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
            LOG.info("Database: {}, table: {}",
                     tableMetaModel.databaseName,
                     tableMetaModel.tableName);
          }
        } else {
          LOG.info("No pending table or partition found");
        }

        TableSplitter tableSplitter = new TableSplitter(pendingTables, mmaMetaManager);
        List<Task> newTasks = tableSplitter.generateTasks(actions);

        tasks.addAll(newTasks);
        LOG.info("Current Tasks: {}", tasks);

        try {
          Thread.sleep(GET_PENDING_TABLE_INTERVAL_MS);
        } catch (InterruptedException e) {
          LOG.warn("Task generating thread was interrupted");
        }
      } catch (MmaException e) {
        LOG.error(ExceptionUtils.getStackTrace(e));
      }
    }
    shutdown();
  }

  @VisibleForTesting
  public void initActions(DataSource dataSource) {
    actions.add(Action.ODPS_DROP_TABLE);
    actions.add(Action.ODPS_CREATE_TABLE);
    actions.add(Action.ODPS_DROP_PARTITION);
    actions.add(Action.ODPS_ADD_PARTITION);
    switch (dataSource) {
      case Hive:
        actions.add(Action.HIVE_LOAD_DATA);
        actions.add(Action.HIVE_SOURCE_VERIFICATION);
        break;
      case ODPS:
        actions.add(Action.ODPS_LOAD_DATA);
        actions.add(Action.ODPS_SOURCE_VERIFICATION);
        break;
      default:
        throw new IllegalArgumentException("Unsupported datasource: " + dataSource);
    }
    actions.add(Action.ODPS_DESTINATION_VERIFICATION);
    actions.add(Action.VERIFICATION);

    LOG.info("Actions initialized");
  }

  @VisibleForTesting
  public SortedSet<Action> getActions() {
    return actions;
  }

  private void initTaskRunner() {
    for (Action action : actions) {
      //Create task runner.
      RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
      LOG.info("Initializing {} for {}", runnerType, action);
      if (!taskRunnerMap.containsKey(runnerType)) {
        taskRunnerMap.put(runnerType, createTaskRunner(runnerType));
        LOG.info("TaskRunner {} created for {}", taskRunnerMap.get(runnerType).getClass(), action);
      }
    }
  }

  private TaskRunner createTaskRunner(RunnerType runnerType) {
    if (RunnerType.HIVE.equals(runnerType)) {
      return new HiveRunner(this.mmaServerConfig.getHiveConfig());
    } else if (RunnerType.ODPS.equals(runnerType)) {
      return new OdpsRunner(this.mmaServerConfig.getOdpsConfig(), this.mmaServerConfig.getOssConfig());
    } else if (RunnerType.VERIFICATION.equals(runnerType)) {
      return new VerificationRunner();
    }
    throw new RuntimeException("Unknown runner type: " + runnerType.name());
  }

  private TaskRunner getTaskRunner(RunnerType runnerType) {
    return taskRunnerMap.get(runnerType);
  }

  private void updateConcurrencyThreshold() {
    actionScheduleInfoMap.put(
        Action.ODPS_DROP_TABLE,
        new ActionScheduleInfo(DROP_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_CREATE_TABLE,
        new ActionScheduleInfo(CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_CREATE_EXTERNAL_TABLE,
        new ActionScheduleInfo(CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_DROP_PARTITION,
        new ActionScheduleInfo(DROP_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_ADD_PARTITION,
        new ActionScheduleInfo(ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION,
        new ActionScheduleInfo(ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_LOAD_DATA,
        new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.HIVE_LOAD_DATA,
        new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_SOURCE_VERIFICATION,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.ODPS_DESTINATION_VERIFICATION,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.HIVE_SOURCE_VERIFICATION,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(
        Action.VERIFICATION,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));

    for (Map.Entry<Action, ActionScheduleInfo> entry : actionScheduleInfoMap.entrySet()) {
      LOG.info("Set concurrency limit for Action: {}, limit: {}",
               entry.getKey().name(),
               entry.getValue().concurrencyLimit);
    }
  }

  private static class ActionScheduleInfo {

    int concurrency;
    int concurrencyLimit;

    ActionScheduleInfo(int concurrencyLimit) {
      this.concurrency = 0;
      this.concurrencyLimit = concurrencyLimit;
    }
  }

  private class SchedulerHeartbeatThread extends Thread {

    public SchedulerHeartbeatThread() {
      super("scheduler thread");
    }

    @Override
    public void run() {
      LOG.info("Heartbeat thread starts");
      while (keepRunning) {
        try {
          synchronized (tasks) {
            for (Action action : actions) {
              LOG.info("Scheduling action: {}, concurrency: {}",
                       action,
                       actionScheduleInfoMap.get(action).concurrency);
              scheduleExecutionTask(action);
            }
          }
        } catch (Throwable ex) {
          LOG.error("Exception on heartbeat", ex);
          ex.printStackTrace();
          savedException = ex;
          // interrupt handler thread in case it waiting on the queue
          break;
        }

        try {
          Thread.sleep(heartbeatIntervalMs.get());
        } catch (InterruptedException ex) {
          LOG.error("Heartbeat interrupted " + ex.getMessage());
        }
      }

      LOG.info("Heartbeat thread ends");
    }
  }

  private void scheduleExecutionTask(Action action) {
    ActionScheduleInfo actionScheduleInfo = actionScheduleInfoMap.get(action);
    if (actionScheduleInfo != null) {
      actionScheduleInfo.concurrency = Math.toIntExact(
          tasks
              .stream()
              .filter(task -> task.actionInfoMap.containsKey(action)
                  && Progress.RUNNING.equals(task.actionInfoMap.get(action).progress))
              .count());

      // If current concurrency exceeds limitation, do not schedule this time
      if (actionScheduleInfo.concurrency >= actionScheduleInfo.concurrencyLimit) {
        LOG.info("Concurrency limitation exceeded, action: {}, current: {}, limitation: {}",
                 action.name(),
                 actionScheduleInfo.concurrency,
                 actionScheduleInfo.concurrencyLimit);
        return;
      }
    }

    for (Task task : tasks) {
      // Skip if terminated
      if (Progress.SUCCEEDED.equals(task.progress) || Progress.FAILED.equals(task.progress)) {
        continue;
      }

      // Check if this action can be scheduled
      if (!task.isReadyAction(action)) {
        continue;
      }

      try {
        task.updateActionProgress(action, Progress.RUNNING);
        LOG.info("Task {} - Action {} submitted to task runner.",
                 task.getName(),
                 action.name());

        getTaskRunner(CommonUtils.getRunnerTypeByAction(action))
            .submitExecutionTask(task, action);
      } catch (MmaException e) {
        LOG.error("Submit task failed", e);
      }
      actionScheduleInfo.concurrency++;
    }
  }

  private void shutdown() {
    LOG.info("Shutdown task runners.");

    for (TaskRunner runner : taskRunnerMap.values()) {
      runner.shutdown();
    }
  }

  private static class ActionComparator implements Comparator<Action> {

    @Override
    public int compare(Action o1, Action o2) {
      if (o1.getPriority() != o2.getPriority()) {
        return o1.getPriority() - o2.getPriority();
      }
      return o1.ordinal() - o2.ordinal();
    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Option config = Option
        .builder("config")
        .longOpt("config")
        .argName("config")
        .hasArg()
        .desc("Specify config.json, default: ./config.json")
        .build();
    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();
    Options options = new Options()
        .addOption(config)
        .addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP)) {
      logHelp(options);
      System.exit(0);
    }

    if (!cmd.hasOption("config")) {
      throw new IllegalArgumentException("Required argument 'config'");
    }

    Path mmaServerConfigPath = Paths.get(cmd.getOptionValue("config"));
    MmaServerConfig mmaServerConfig = MmaServerConfig.fromFile(mmaServerConfigPath);
    if (!mmaServerConfig.validate()) {
      System.err.println("Invalid mma server config: " + mmaServerConfig.toJson());
      System.exit(1);
    }

    TaskScheduler scheduler = new TaskScheduler();
    try {
      scheduler.run(mmaServerConfig);
    } finally {
      scheduler.shutdown();
    }
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "migrate --config <config.json>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
