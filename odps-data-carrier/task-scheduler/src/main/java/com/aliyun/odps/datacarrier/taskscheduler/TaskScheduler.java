package com.aliyun.odps.datacarrier.taskscheduler;


import static com.aliyun.odps.datacarrier.taskscheduler.Constants.HELP;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.META_CONFIG_FILE;

import java.io.File;
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

  private TaskManager taskManager;
//  private DataValidator dataValidator;
  private Map<Action, ActionScheduleInfo> actionScheduleInfoMap;
  private SortedSet<Action> actions;
  protected Map<RunnerType, TaskRunner> taskRunnerMap;
  private DataSource dataSource;
  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected List<Task> tasks;
  private MetaConfiguration metaConfig;
  private MMAMetaManager mmaMetaManager;

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
//    this.dataValidator = new DataValidator();
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = new LinkedList<>();
  }

  private void run(MetaConfiguration metaConfiguration) throws TException {

    // TODO: check if datasource and metasource are valid
    this.metaConfig = metaConfiguration;
    this.dataSource = metaConfiguration.getDataSource();
    MetaConfiguration.HiveConfiguration hiveConfigurationConfig = metaConfiguration.getHiveConfiguration();
    MetaSource metaSource = new HiveMetaSource(hiveConfigurationConfig.getThriftAddr(),
                                               hiveConfigurationConfig.getKrbPrincipal(),
                                               hiveConfigurationConfig.getKeyTab(),
                                               hiveConfigurationConfig.getKrbSystemProperties());
    this.mmaMetaManager = new MMAMetaManagerFsImpl(null, metaSource);
    for (MetaConfiguration.TableGroup tableGroup : metaConfiguration.getTableGroups()) {
      for (MetaConfiguration.TableConfig tableConfig : tableGroup.getTableConfigs()) {
        this.mmaMetaManager.initMigration(tableConfig);
      }
    }

    initActions(this.dataSource);
    initTaskRunner();
    updateConcurrencyThreshold();
    this.heartbeatThread.start();
    int retryTimes = 1;
    while (keepRunning) {
      LOG.info("Start to migrate data for the [{}] round", retryTimes);
      List<MetaSource.TableMetaModel> pendingTables = this.mmaMetaManager.getPendingTables();
      LOG.info("Tables to migrate");
      for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
        LOG.info("Database: {}, table: {}",
                 tableMetaModel.databaseName,
                 tableMetaModel.tableName);
      }

      this.taskManager = new TableSplitter(pendingTables, metaConfiguration);
      this.tasks.clear();
      this.tasks.addAll(this.taskManager.generateTasks(actions, null));

      if (this.tasks.isEmpty()) {
        LOG.info("No tasks to schedule");
        try {
          Thread.sleep(GET_PENDING_TABLE_INTERVAL);
        } catch (InterruptedException e) {
          // ignore
        }
      } else {
        LOG.info("Add {} tasks in the [{}] round", tasks.size(), retryTimes);
      }

      while (!isAllTasksFinished()) {
        try {
          Thread.sleep(HEARTBEAT_INTERVAL_MS);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      LOG.info("All tasks finished");
      retryTimes++;
    }
    shutdown();
  }

  @VisibleForTesting
  public void initActions(DataSource dataSource) {
    if (DataSource.Hive.equals(dataSource)) {
      actions.add(Action.ODPS_CREATE_TABLE);
      actions.add(Action.ODPS_ADD_PARTITION);
      actions.add(Action.HIVE_LOAD_DATA);
//      actions.add(Action.HIVE_VALIDATE);
//      actions.add(Action.ODPS_VALIDATE);
//      actions.add(Action.VALIDATION_BY_PARTITION);
//      actions.add(Action.VALIDATION_BY_TABLE);
//    } else if (DataSource.OSS.equals(dataSource)) {
//      actions.add(Action.ODPS_CREATE_TABLE);
//      actions.add(Action.ODPS_ADD_PARTITION);
//      actions.add(Action.ODPS_CREATE_EXTERNAL_TABLE);
//      actions.add(Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION);
//      actions.add(Action.ODPS_LOAD_DATA);
//      actions.add(Action.ODPS_VALIDATE);
//      actions.add(Action.VALIDATION_BY_TABLE);
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
      return new HiveRunner(this.metaConfig.getHiveConfiguration());
    } else if (RunnerType.ODPS.equals(runnerType)) {
      return new OdpsRunner(this.metaConfig.getOdpsConfiguration());
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
    actionScheduleInfoMap.put(Action.ODPS_VALIDATE,
                              new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.HIVE_VALIDATE,
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

    public void run() {
      LOG.info("Heartbeat thread starts");
      while(keepRunning) {
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
          LOG.error("Heartbeat interrupted "+ ex.getMessage());
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

    // Print logs
    for (Task task : tasks) {
      StringBuilder csb = new StringBuilder(task.getTaskId());
      StringBuilder sb = new StringBuilder(task.getTaskId());
      csb.append(":").append(task.progress.toColorString()).append("--> ");
      sb.append(":").append(task.progress).append("--> ");
      for (Action action : actions) {
        if (!task.actionInfoMap.containsKey(action)) {
          continue;
        }
        csb.append(action.name()).append("(").append(task.actionInfoMap.get(action).progress.toColorString()).append(") ");
        sb.append(action.name()).append("(").append(task.actionInfoMap.get(action).progress).append(") ");
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
      // TODO: this is quite hacky and not efficient, should consider a better design
      // Skip if this table has reached final state
      MMAMetaManager.MigrationStatus status = mmaMetaManager.getStatus(task.getSourceDatabaseName(),
                                                                       task.getSourceTableName());
      if (MMAMetaManager.MigrationStatus.SUCCEEDED.equals(status) ||
          MMAMetaManager.MigrationStatus.FAILED.equals(status)) {
        continue;
      }

      // Update progress if task succeeded or failed
      if (Progress.SUCCEEDED.equals(task.progress)) {
        mmaMetaManager.updateStatus(task.getSourceDatabaseName(),
                                    task.getSourceTableName(),
                                    MMAMetaManager.MigrationStatus.SUCCEEDED);
        continue;
      } else if (Progress.FAILED.equals(task.progress)) {
        mmaMetaManager.updateStatus(task.getSourceDatabaseName(),
                                    task.getSourceTableName(),
                                    MMAMetaManager.MigrationStatus.FAILED);
        continue;
      }

      // Check if this action can be scheduled
      if (!task.isReadyAction(action)) {
        continue;
      }

      LOG.info("Task {} - Action {} is ready to schedule.", task.getTaskId(), action.name());

      // Schedule
      for (Map.Entry<String, AbstractExecutionInfo> entry :
          task.actionInfoMap.get(action).executionInfoMap.entrySet()) {
        if (!Progress.NEW.equals(entry.getValue().progress)) {
          continue;
        }
        String executionTaskName = entry.getKey();
        task.changeExecutionProgress(action, executionTaskName, Progress.RUNNING);
        LOG.info("Task {} - Action {} - Execution {} submitted to task runner.",
                 task.getTaskId(), action.name(), executionTaskName);
        getTaskRunner(CommonUtils.getRunnerTypeByAction(action)).submitExecutionTask(task, action, executionTaskName);
        actionScheduleInfo.concurrency++;
      }
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
      return o1.ordinal() - o2.ordinal();
    }
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Option config = Option
        .builder("config")
        .longOpt(META_CONFIG_FILE)
        .argName(META_CONFIG_FILE)
        .hasArg()
        .desc("Specify config.json, default: ./config.json")
        .build();
    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();
    Option version = Option
        .builder("v")
        .longOpt("version")
        .argName("version")
        .hasArg(false)
        .desc("Print MMA version")
        .build();
    Options options = new Options()
        .addOption(config)
        .addOption(help)
        .addOption(version);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("version")) {
      System.err.println("0.0.1");
      System.exit(0);
    }

    if (!cmd.hasOption(HELP)) {
      // TODO: use a fixed parent directory
      File configFile = new File(System.getProperty("user.dir"), META_CONFIG_FILE);
      if (cmd.hasOption(META_CONFIG_FILE)) {
        configFile = new File(cmd.getOptionValue(META_CONFIG_FILE));
      }
      MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
      if (!metaConfiguration.validateAndInitConfig()) {
        LOG.error("Init MetaConfiguration failed, please check {}", configFile.toString());
        System.exit(1);
      }
      TaskScheduler scheduler = new TaskScheduler();
      try {
        scheduler.run(metaConfiguration);
      } finally {
        scheduler.shutdown();
      }
    } else {
      logHelp(options);
    }
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "migrate -config <config.json>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
