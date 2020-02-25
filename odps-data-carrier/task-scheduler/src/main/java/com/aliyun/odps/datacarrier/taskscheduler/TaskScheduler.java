package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.metacarrier.HiveMetaSource;
import com.aliyun.odps.datacarrier.metacarrier.MetaSource;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.*;
import static com.aliyun.odps.datacarrier.taskscheduler.MetaConfigurationUtils.*;

public class TaskScheduler {

  private static final Logger LOG = LogManager.getLogger(TaskScheduler.class);

  private static final int HEARTBEAT_INTERVAL_MS = 3000;
  private static final int CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT = 10;

  private TaskManager taskManager;
  private DataValidator dataValidator;
  private Map<Action, ActionScheduleInfo> actionScheduleInfoMap;
  private SortedSet<Action> actions;
  protected Map<RunnerType, TaskRunner> taskRunnerMap;
  private DataSource dataSource;
  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected List<TableMetaModel> tables;
  protected List<Task> tasks;
  private MetaConfiguration metaConfig;

  //for HiveRunner.
  private String user;
  private String password;

  private MMAMetaManager mmaMetaManager;
  private MetaSource metaSource;

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
    this.dataValidator = new DataValidator();
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = new LinkedList<>();
  }


  private void run(MetaConfiguration metaConfiguration, String user, String password) throws TException {
    this.metaConfig = metaConfiguration;
    this.dataSource = metaConfiguration.getDataSource();
    this.user = user;
    this.password = password;

    int retryTimes = 1;
    while (true) {
      LOG.info("Start to migrate data for the [{}] round. ", retryTimes);
      initActions(this.dataSource);
      initTaskRunner();
      updateConcurrencyThreshold();
      if (DataSource.Hive.equals(dataSource)) {
        MetaConfiguration.HiveConfiguration hiveConfigurationConfig = metaConfiguration.getHiveConfiguration();
        this.metaSource = new HiveMetaSource(hiveConfigurationConfig.getThriftAddr(),
            hiveConfigurationConfig.getKrbPrincipal(),
            hiveConfigurationConfig.getKeyTab(),
            hiveConfigurationConfig.getKrbSystemProperties());
        this.mmaMetaManager = new MMAMetaManagerFsImpl(null, this.metaSource);
      }
      this.tables = this.mmaMetaManager.getPendingTables();
      this.taskManager = new TableSplitter(this.tables, metaConfiguration);
      this.tasks.clear();
      this.tasks.addAll(this.taskManager.generateTasks(actions, null));

      if (this.tasks.isEmpty()) {
        LOG.info("None tasks to be scheduled， migration done.");
        return;
      } else {
        LOG.info("Add {} tasks in the [{}] round.", tasks.size(), retryTimes);
      }
      this.heartbeatThread.start();
      retryTimes++;
    }
  }

  @VisibleForTesting
  public void initActions(DataSource dataSource) {
    if (DataSource.Hive.equals(dataSource)) {
      actions.add(Action.ODPS_CREATE_TABLE);
      actions.add(Action.ODPS_ADD_PARTITION);
      actions.add(Action.HIVE_LOAD_DATA);
      actions.add(Action.HIVE_VALIDATE);
      actions.add(Action.ODPS_VALIDATE);
      actions.add(Action.VALIDATION_BY_PARTITION);
    } else if (DataSource.OSS.equals(dataSource)) {
      actions.add(Action.ODPS_CREATE_TABLE);
      actions.add(Action.ODPS_ADD_PARTITION);
      actions.add(Action.ODPS_CREATE_EXTERNAL_TABLE);
      actions.add(Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION);
      actions.add(Action.ODPS_LOAD_DATA);
      actions.add(Action.ODPS_VALIDATE);
      actions.add(Action.VALIDATION_BY_TABLE);
    }
  }

  @VisibleForTesting
  public SortedSet<Action> getActions() {
    return actions;
  }

  private void initTaskRunner() {
    for (Action action : this.actions) {
      //Create task runner.
      RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
      if (!taskRunnerMap.containsKey(runnerType)) {
        taskRunnerMap.put(runnerType, createTaskRunner(runnerType));
        LOG.info("Find runnerType = {}, Add Runner: {}", runnerType, taskRunnerMap.get(runnerType).getClass());
      }
    }
  }

  private TaskRunner createTaskRunner(RunnerType runnerType) {
    if (RunnerType.HIVE.equals(runnerType)) {
      return new HiveRunner(this.metaConfig.getHiveConfiguration().getHiveJdbcAddress(), this.user, this.password);
    } else if (RunnerType.ODPS.equals(runnerType)) {
      return new OdpsRunner();
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
        new ActionScheduleInfo(CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_ADD_PARTITION,
        new ActionScheduleInfo(ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION,
        new ActionScheduleInfo(ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_LOAD_DATA,
        new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.HIVE_LOAD_DATA,
        new ActionScheduleInfo(LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.ODPS_VALIDATE,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));
    actionScheduleInfoMap.put(Action.HIVE_VALIDATE,
        new ActionScheduleInfo(VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT));

    for (Map.Entry<Action, ActionScheduleInfo> entry : actionScheduleInfoMap.entrySet()) {
      LOG.info("Set concurrency limit for Action: {}, limit: {}", entry.getKey().name(), entry.getValue().concurrencyLimit);
    }
  }


  private class ActionScheduleInfo {
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
      LOG.info("Start scheduling tasks...");
      while(true) {
        if (!keepRunning) {
          return;
        }
        try {
          if (isAllTasksFinished()) {
            LOG.info("All tasks finished, heartbeat will stop.");
            keepRunning = false;
            break;
          }

          for (Action action : actions) {
            scheduleExecutionTask(action);
          }

        } catch (Throwable ex) {
          LOG.error("Exception on heartbeat " + ex.getMessage());
          ex.printStackTrace();
          savedException = ex;
          // interrupt handler thread in case it waiting on the queue
          return;
        }

        try {
          Thread.sleep(heartbeatIntervalMs.get());
        } catch (InterruptedException ex) {
          LOG.error("Heartbeat interrupted "+ ex.getMessage());
        }
      }
      shutdown();
      LOG.info("Finish all tasks.");
    }
  }

  private boolean isAllTasksFinished() {
    if (tasks.isEmpty()) {
      LOG.info("None tasks to be scheduled.");
      return true;
    }
    for (Task task : tasks) {
      StringBuilder csb = new StringBuilder(task.toString());
      StringBuilder sb = new StringBuilder(task.toString());
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
      System.out.print(csb.toString() + "\n");
    }
    return tasks.stream().allMatch(task -> Progress.FAILED.equals(task.progress)
        || Progress.SUCCEEDED.equals(task.progress));
  }



  private void scheduleExecutionTask(Action action) {
    ActionScheduleInfo actionScheduleInfo = actionScheduleInfoMap.get(action);
    if (actionScheduleInfo != null) {
      actionScheduleInfo.concurrency = Math.toIntExact(tasks.stream()
          .filter(task -> task.actionInfoMap.containsKey(action)
              && Progress.RUNNING.equals(task.actionInfoMap.get(action).progress))
          .count());

      LOG.info("Action: {}, concurrency: {}, concurrencyLimit: {}",
          action.name(), actionScheduleInfo.concurrency, actionScheduleInfo.concurrencyLimit);
      if (actionScheduleInfo.concurrency >= actionScheduleInfo.concurrencyLimit) {
        return;
      }
    }

    for (Task task : tasks) {
      if (!task.isReadyAction(action)) {
        continue;
      }
      LOG.info("Task {} - Action {} is ready to schedule.", task.toString(), action.name());
      if (Action.VALIDATION_BY_TABLE.equals(action)) {
        if (dataValidator.validateTaskCountResult(task)) {
          task.changeActionProgress(action, Progress.SUCCEEDED);
          mmaMetaManager.updateStatus(task.project, task.tableName, MMAMetaManager.MigrationStatus.SUCCEEDED);
        } else {
          task.changeActionProgress(action, Progress.FAILED);
          mmaMetaManager.updateStatus(task.project, task.tableName, MMAMetaManager.MigrationStatus.FAILED);
        }
      } else if (Action.VALIDATION_BY_PARTITION.equals(action)) {
        DataValidator.ValidationResult validationResult = dataValidator.failedValidationPartitions(task);
        if (!validationResult.failedPartitions.isEmpty()) {
          task.changeActionProgress(action, Progress.FAILED);
          mmaMetaManager.updateStatus(task.project, task.tableName,
              validationResult.failedPartitions, MMAMetaManager.MigrationStatus.FAILED);
        } else {
          task.changeActionProgress(action, Progress.SUCCEEDED);
        }
        if (!validationResult.succeededPartitions.isEmpty()) {
          mmaMetaManager.updateStatus(task.project, task.tableName,
              validationResult.succeededPartitions, MMAMetaManager.MigrationStatus.SUCCEEDED);
        }
      } else {
        for (Map.Entry<String, AbstractExecutionInfo> entry :
            task.actionInfoMap.get(action).executionInfoMap.entrySet()) {
          if (!Progress.NEW.equals(entry.getValue().progress)) {
            continue;
          }
          String executionTaskName = entry.getKey();
          task.changeExecutionProgress(action, executionTaskName, Progress.RUNNING);
          LOG.info("Task {} - Action {} - Execution {} submitted to task runner.",
              task.toString(), action.name(), executionTaskName);
          getTaskRunner(CommonUtils.getRunnerTypeByAction(action)).submitExecutionTask(task, action, executionTaskName);
          actionScheduleInfo.concurrency++;
        }
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
    // for hive JDBC
    Option user = Option
        .builder("u")
        .longOpt(USER)
        .argName(USER)
        .hasArg()
        .desc("JDBC UserName, default value as \"Hive\"")
        .build();
    Option password = Option
        .builder("p")
        .longOpt(PASSWORD)
        .argName(PASSWORD)
        .optionalArg(true)
        .hasArg()
        .desc("JDBC Password, default value as \"\"")
        .build();
    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();

    Options options = new Options()
        .addOption(config)
        .addOption(user)
        .addOption(password)
        .addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(USER) && cmd.hasOption(PASSWORD) && !cmd.hasOption(HELP)) {
      File configFile = getDefaultConfigFile();
      if (cmd.hasOption(META_CONFIG_FILE)) {
        configFile = new File(cmd.getOptionValue(META_CONFIG_FILE));
      }
      MetaConfiguration metaConfiguration = readConfigFile(configFile);
      if (!metaConfiguration.validateAndInitConfig()) {
        LOG.error("Init MetaConfiguration failed, please check {}", configFile.toString());
        System.exit(1);
      }
      TaskScheduler scheduler = new TaskScheduler();
      String cmdUser = "hive";
      String cmdPassword = "";
      if (!StringUtils.isNullOrEmpty(cmd.getOptionValue(USER))) {
        cmdUser = cmd.getOptionValue(USER);
      }
      if (!StringUtils.isNullOrEmpty(cmd.getOptionValue(PASSWORD))) {
        cmdPassword = cmd.getOptionValue(PASSWORD);
      }
      scheduler.run(metaConfiguration, cmdUser, cmdPassword);
    } else {
      logHelp(options);
    }
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "task-scheduler -i <input directory> -d <datasource> -m <mode>";
    formatter.printHelp(cmdLineSyntax, options);
  }
}
