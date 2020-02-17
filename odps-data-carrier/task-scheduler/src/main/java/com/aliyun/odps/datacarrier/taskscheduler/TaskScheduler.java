package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.metacarrier.HiveMetaCarrier;
import com.aliyun.odps.datacarrier.metacarrier.MetaCarrier;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.*;

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
  private String tableMappingFilePath;

  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected List<TableMetaModel> tables;
  protected List<Task> tasks;
  protected Set<String> finishedTasks;

  //for HiveRunner.
  private String jdbcAddress;
  private String user;
  private String password;

  private MetaCarrier metaCarrier;

  private TaskMetaManager taskMetaManager;

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
    this.dataValidator = new DataValidator();
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = new LinkedList<>();
    this.finishedTasks = new HashSet<>();
  }


  private void run(String inputPath, DataSource dataSource, Mode mode, String tableMappingFilePath,
                   String jdbcAddress, String user, String password, String where, String hmsThriftAddress,
                   String hmsPrincipal, String hmsKeyTab, String[] hmsSystemProperties,
                   String numOfPartitions, String retryTimeThreshold)
      throws IOException, TException {

    this.dataSource = dataSource;
    this.jdbcAddress = jdbcAddress;
    this.user = user;
    this.password = password;
    this.taskMetaManager = new TaskMetaManager();

    int retryTimes = 0;
    while (retryTimes < Integer.valueOf(retryTimeThreshold)) {
      LOG.info("Start to migrate data for the [{}] round. ", retryTimes);
      initActions(this.dataSource);
      initTaskRunner();
      updateConcurrencyThreshold();
      if (DataSource.Hive.equals(dataSource)) {
        this.metaCarrier = new HiveMetaCarrier(hmsThriftAddress, null, hmsPrincipal, hmsKeyTab, hmsSystemProperties);
        this.tables = this.metaCarrier.generateMetaModelWithFailover(tableMappingFilePath, this.taskMetaManager.getSucceededTableMeta());
      }
      this.taskManager = new TableSplitter(this.tables, Integer.valueOf(numOfPartitions));
      this.tasks.clear();
      this.tasks.addAll(this.taskManager.generateTasks(actions, mode));

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
      actions.add(Action.VALIDATION);
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
      return new HiveRunner(this.jdbcAddress, this.user, this.password);
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
    } else if (!Action.VALIDATION.equals(action)) {
      LOG.error("Action {} scheduleInfo is not found.", action.name());
      return;
    }

    for (Task task : tasks) {
      if (!task.isReadyAction(action)) {
        continue;
      }
      LOG.info("Task {} - Action {} is ready to schedule.", task.toString(), action.name());
      if (Action.VALIDATION.equals(action) || Action.VALIDATION_BY_PARTITION.equals(action)) {
        //TODO validateTaskCountResult per partition.
        if ((Action.VALIDATION.equals(action) && dataValidator.validateTaskCountResult(task))
          || (Action.VALIDATION_BY_PARTITION.equals(action) && dataValidator.validateTaskCountResultByPartition(task).isEmpty())) {
          task.changeActionProgress(action, Progress.SUCCEEDED);

          // tasks done, write to failover file.
          if (Progress.SUCCEEDED.equals(task.progress) && finishedTasks.add(task.getTableNameWithProject())) {
            // TODO, update validation succeeded partitions.
            taskMetaManager.writeToFailoverFile(task.getTableNameWithProject() + "\n");
          }
        } else {
          task.changeActionProgress(action, Progress.FAILED);
          if (Action.VALIDATION_BY_PARTITION.equals(action)) {
            // TODO, update validation succeeded partitions.
            // mmaMetaManager.updateTaskInfo();
          }
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
    Option meta = Option
        .builder("i")
        .longOpt(INPUT_DIR)
        .argName(INPUT_DIR)
        .hasArg()
        .desc("Directory generated by meta processor")
        .build();
    Option datasource = Option
        .builder("d")
        .longOpt(DATA_SOURCE)
        .argName(DATA_SOURCE)
        .hasArg()
        .desc("Specify datasource, can be Hive or OSS")
        .build();
    Option mode = Option
        .builder("m")
        .longOpt(MODE)
        .argName(MODE)
        .hasArg()
        .desc("Migration mode, SINGLE or BATCH.")
        .build();

    Option tableMapping = Option
        .builder("tm")
        .longOpt(TABLE_MAPPING)
        .argName(TABLE_MAPPING)
        .hasArg()
        .desc("The path of table mapping from Hive to MaxCompute in BATCH mode.")
        .build();

    // for hive JDBC
    Option jdbcAddress = Option
        .builder("ja")
        .longOpt(JDBC_ADDRESS)
        .argName(JDBC_ADDRESS)
        .hasArg()
        .desc("JDBC Address for Hive Runner, e.g. jdbc:hive2://127.0.0.1:10000/default")
        .build();
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
    Option where = Option
        .builder("w")
        .longOpt(WHERE)
        .argName(WHERE)
        .hasArg()
        .desc("where condition")
        .build();

    // for hive metastore
    Option uri = Option
        .builder("u")
        .longOpt(HIVE_META_THRIFT_ADDRESS)
        .argName(HIVE_META_THRIFT_ADDRESS)
        .hasArg()
        .desc("Required, hive metastore thrift uri, e.g. thrift://127.0.0.1:9083")
        .build();
    Option principal = Option
        .builder()
        .longOpt(HIVE_META_STORE_PRINCIPAL)
        .argName(HIVE_META_STORE_PRINCIPAL)
        .hasArg()
        .desc("Optional, hive metastore's Kerberos principal")
        .build();
    Option keyTab = Option
        .builder()
        .longOpt(HIVE_META_STORE_KEY_TAB)
        .argName(HIVE_META_STORE_KEY_TAB)
        .hasArg()
        .desc("Optional, hive metastore's Kerberos keyTab")
        .build();
    Option systemProperties = Option
        .builder()
        .longOpt(HIVE_META_STORE_SYSTEM)
        .argName(HIVE_META_STORE_SYSTEM)
        .hasArg()
        .numberOfArgs(Option.UNLIMITED_VALUES)
        .desc("system properties")
        .build();

    Option numOfPartitionsOpt = Option
        .builder("np")
        .longOpt(NUM_OF_PARTITIONS)
        .argName(NUM_OF_PARTITIONS)
        .hasArgs()
        .desc("Optional, specify number of partitions to split table.")
        .build();

    Option retryTimeThresholdOpt = Option
        .builder("rtt")
        .longOpt(RETRY_TIME_THRESHOLD)
        .argName(RETRY_TIME_THRESHOLD)
        .hasArgs()
        .desc("Optional, specify upper limit of retry times for failed tables")
        .build();

    Option help = Option
        .builder("h")
        .longOpt(HELP)
        .argName(HELP)
        .desc("Print help information")
        .build();

    Options options = new Options()
        .addOption(meta)
        .addOption(datasource)
        .addOption(mode)
        .addOption(tableMapping)
        .addOption(jdbcAddress)
        .addOption(user)
        .addOption(password)
        .addOption(where)
        .addOption(uri)
        .addOption(principal)
        .addOption(keyTab)
        .addOption(systemProperties)
        .addOption(numOfPartitionsOpt)
        .addOption(retryTimeThresholdOpt)
        .addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(INPUT_DIR)
        && cmd.hasOption(DATA_SOURCE)
        && cmd.hasOption(MODE)
        && cmd.hasOption(TABLE_MAPPING)
        && cmd.hasOption(HIVE_META_THRIFT_ADDRESS)
        && !cmd.hasOption(HELP)) {
      TaskScheduler scheduler = new TaskScheduler();
      DataSource cmdDataSource = DataSource.Hive;
      Mode cmdMode = Mode.BATCH;
      try {
        cmdDataSource = DataSource.valueOf(cmd.getOptionValue(DATA_SOURCE));
        cmdMode = Mode.valueOf(cmd.getOptionValue(MODE));
      } catch (IllegalArgumentException e) {
        logHelp(options);
      }
      String cmdJdbcAddress = "jdbc:hive2://127.0.0.1:10000/default";
      String cmdUser = "hive";
      String cmdPassword = "";
      String whereStr = "";
      if (!cmd.hasOption(JDBC_ADDRESS) || !cmd.hasOption(USER) || !cmd.hasOption(PASSWORD)) {
        System.out.print("Has not set JDBC Info(include jdbc-address, user and password), run TaskScheduler as TEST!\n");
      }
      if (cmd.hasOption(JDBC_ADDRESS) && !StringUtils.isNullOrEmpty(cmd.getOptionValue(JDBC_ADDRESS))) {
        cmdJdbcAddress = cmd.getOptionValue(JDBC_ADDRESS);
      }
      if (cmd.hasOption(USER) && !StringUtils.isNullOrEmpty(cmd.getOptionValue(USER))) {
        cmdUser = cmd.getOptionValue(USER);
      }
      if (cmd.hasOption(PASSWORD) && !StringUtils.isNullOrEmpty(cmd.getOptionValue(PASSWORD))) {
        cmdPassword = cmd.getOptionValue(PASSWORD);
      }
      if (cmd.hasOption(WHERE) && !StringUtils.isNullOrEmpty(cmd.getOptionValue(WHERE))) {
        whereStr = cmd.getOptionValue(WHERE);
      }
      String principalVal = cmd.getOptionValue(HIVE_META_STORE_PRINCIPAL);
      String keyTabVal = cmd.getOptionValue(HIVE_META_STORE_KEY_TAB);
      String[] systemPropertiesValue = cmd.getOptionValues(HIVE_META_STORE_SYSTEM);
      scheduler.run(cmd.getOptionValue(INPUT_DIR), cmdDataSource, cmdMode, cmd.getOptionValue(TABLE_MAPPING),
          cmdJdbcAddress, cmdUser, cmdPassword, whereStr, cmd.getOptionValue(HIVE_META_THRIFT_ADDRESS), principalVal,
          keyTabVal, systemPropertiesValue, cmd.getOptionValue(NUM_OF_PARTITIONS), cmd.getOptionValue(RETRY_TIME_THRESHOLD));
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
