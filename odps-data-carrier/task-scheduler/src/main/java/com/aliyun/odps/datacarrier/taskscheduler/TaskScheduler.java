package com.aliyun.odps.datacarrier.taskscheduler;


import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;

// TODO: move main to a new class MmaServerMain
public class TaskScheduler {

  private static final Logger LOG = LogManager.getLogger(TaskScheduler.class);

  private static final int GET_PENDING_TABLE_INTERVAL = 30000;
  private static final int HEARTBEAT_INTERVAL_MS = 3000;
  private static final int CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT = 10;

  private Map<Action, ActionScheduleInfo> actionScheduleInfoMap;
  private SortedSet<Action> actions;
  protected Map<RunnerType, TaskRunner> taskRunnerMap;
  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected List<Task> tasks;
  private MmaServerConfig mmaServerConfig;

  // This map indicates if a table succeeded in current round
  // database -> table -> status
  private static Map<String, Map<String, Boolean>> databaseTableStatus = new ConcurrentHashMap<>();

  private static void initDatabaseTableStatus(List<MetaSource.TableMetaModel> pendingTables) {
    databaseTableStatus.clear();
    for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
      databaseTableStatus
          .computeIfAbsent(tableMetaModel.databaseName, k -> new ConcurrentHashMap<>())
          .put(tableMetaModel.tableName, true);
    }
  }

  public static void markAsFailed(String db, String tbl) {
    if (!databaseTableStatus.containsKey(db)) {
      throw new IllegalStateException("Failed to update table status: table not found");
    }
    databaseTableStatus.get(db).put(tbl, false);
  }

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = new LinkedList<>();
  }

  private void run(MmaServerConfig mmaServerConfig) throws TException, IOException {

    // TODO: check if datasource and metasource are valid
    this.mmaServerConfig = mmaServerConfig;

    MmaConfig.HiveConfig hiveConfig = mmaServerConfig.getHiveConfig();
    MetaSource metaSource = new HiveMetaSource(hiveConfig.getHmsThriftAddr(),
                                               hiveConfig.getKrbPrincipal(),
                                               hiveConfig.getKeyTab(),
                                               hiveConfig.getKrbSystemProperties());
    MmaMetaManagerFsImpl.init(null, metaSource);

    initActions(mmaServerConfig.getDataSource());
    initTaskRunner();
    updateConcurrencyThreshold();
    this.heartbeatThread.start();
    int round = 1;
    while (keepRunning) {
      // Get tables to migrate
      LOG.info("Start to migrate data for the [{}] round", round);
      List<MetaSource.TableMetaModel>
          pendingTables =
          MmaMetaManagerFsImpl.getInstance().getPendingTables();
      LOG.info("Tables to migrate");
      for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
        LOG.info("Database: {}, table: {}",
                 tableMetaModel.databaseName,
                 tableMetaModel.tableName);
      }

      // Init tableStatus
      initDatabaseTableStatus(pendingTables);

      TableSplitter tableSplitter = new TableSplitter(pendingTables);
      this.tasks.clear();
      this.tasks.addAll(tableSplitter.generateTasks(actions));

      if (this.tasks.isEmpty()) {
        System.err.println("Waiting for migration jobs");
        LOG.info("No tasks to schedule");
        try {
          Thread.sleep(GET_PENDING_TABLE_INTERVAL);
        } catch (InterruptedException e) {
          // ignore
        }
      } else {
        LOG.info("Add {} tasks in the [{}] round", tasks.size(), round);
      }

      while (!isAllTasksFinished()) {
        try {
          Thread.sleep(HEARTBEAT_INTERVAL_MS);
        } catch (InterruptedException e) {
          // ignore
        }
      }

      // Update table status
      for (Map.Entry<String, Map<String, Boolean>> dbEntry: databaseTableStatus.entrySet()) {
        for (Map.Entry<String, Boolean> tableEntry : dbEntry.getValue().entrySet()) {
          if (tableEntry.getValue()) {
            MmaMetaManagerFsImpl
                .getInstance()
                .updateStatus(dbEntry.getKey(), tableEntry.getKey(), MmaMetaManager.MigrationStatus.SUCCEEDED);
          } else {
            MmaMetaManagerFsImpl
                .getInstance()
                .updateStatus(dbEntry.getKey(), tableEntry.getKey(), MmaMetaManager.MigrationStatus.FAILED);
          }
        }
      }
      LOG.info("All tasks finished");
      round++;
    }
    shutdown();
  }

  @VisibleForTesting
  public void initActions(DataSource dataSource) {
    if (DataSource.Hive.equals(dataSource)) {
      actions.add(Action.ODPS_CREATE_TABLE);
      actions.add(Action.ODPS_ADD_PARTITION);
      actions.add(Action.HIVE_LOAD_DATA);
      actions.add(Action.ODPS_VERIFICATION);
      actions.add(Action.HIVE_VERIFICATION);
      actions.add(Action.VERIFICATION);
    } else {
      throw new IllegalArgumentException("Unsupported datasource: " + dataSource);
    }

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
      return new OdpsRunner(this.mmaServerConfig.getOdpsConfig());
    } else if (RunnerType.VERIFICATION.equals(runnerType)) {
      return new VerificationRunner();
    }
    throw new RuntimeException("Unknown runner type: " + runnerType.name());
  }

  private TaskRunner getTaskRunner(RunnerType runnerType) {
    return taskRunnerMap.get(runnerType);
  }

  private void updateConcurrencyThreshold() {
    actionScheduleInfoMap.put(Action.ODPS_CREATE_TABLE,
                              new ActionScheduleInfo(CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_CREATE_EXTERNAL_TABLE,
                              new ActionScheduleInfo(
                                  CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_ADD_PARTITION,
                              new ActionScheduleInfo(ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION,
                              new ActionScheduleInfo(
                                  ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_LOAD_DATA,
                              new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.HIVE_LOAD_DATA,
                              new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_VERIFICATION, new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.HIVE_VERIFICATION, new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.VERIFICATION, new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
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

    public void run() {
      LOG.info("Heartbeat thread starts");
      while (keepRunning) {
        try {
          for (Action action : actions) {
            scheduleExecutionTask(action);
          }
        } catch (Throwable ex) {
          LOG.error("Exception on heartbeat " + ex.getMessage());
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

  private boolean isAllTasksFinished() {
    if (tasks.isEmpty()) {
      LOG.info("None tasks to be scheduled.");
      return true;
    }

    for (Task task : tasks) {
      StringBuilder csb = new StringBuilder(task.getName());
      StringBuilder sb = new StringBuilder(task.getName());
      csb.append(":").append(task.progress.toColorString()).append("--> ");
      sb.append(":").append(task.progress).append("--> ");
      for (Action action : actions) {
        if (!task.actionInfoMap.containsKey(action)) {
          continue;
        }

        AbstractActionInfo executionInfo = task.actionInfoMap.get(action);
        csb
            .append(action.name())
            .append("(").append(executionInfo.progress.toColorString()).append(") ");
        sb
            .append(action.name())
            .append("(").append(executionInfo.progress).append(") ");
      }
      LOG.info(sb.toString());
      System.err.print(csb.toString() + "\n");
    }

    return tasks.stream().allMatch(task -> Progress.FAILED.equals(task.progress)
                                           || Progress.SUCCEEDED.equals(task.progress));
  }

  private void scheduleExecutionTask(Action action) {
    ActionScheduleInfo actionScheduleInfo = actionScheduleInfoMap.get(action);
    if (actionScheduleInfo != null) {
      // Get current concurrency
      actionScheduleInfo.concurrency = Math.toIntExact(tasks.stream()
          .filter(task -> task.actionInfoMap.containsKey(action)
              && Progress.RUNNING.equals(task.actionInfoMap.get(action).progress))
          .count());

      LOG.info("Action: {}, concurrency: {}, concurrencyLimit: {}",
               action.name(), actionScheduleInfo.concurrency, actionScheduleInfo.concurrencyLimit);

      // If current concurrency exceeds limitation, do not schedule this time
      if (actionScheduleInfo.concurrency >= actionScheduleInfo.concurrencyLimit) {
        return;
      }
    }

    // Iterate over tasks, start actions and update status
    for (Task task : tasks) {
      // Skip if terminated
      if (Progress.SUCCEEDED.equals(task.progress) || Progress.FAILED.equals(task.progress)) {
        continue;
      }

      // Check if this action can be scheduled
      if (!task.isReadyAction(action)) {
        continue;
      }

      task.updateActionProgress(action, Progress.RUNNING);
      LOG.info("Task {} - Action {} submitted to task runner.",
          task.getName(), action.name());
      getTaskRunner(CommonUtils.getRunnerTypeByAction(action))
          .submitExecutionTask(task, action);
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
