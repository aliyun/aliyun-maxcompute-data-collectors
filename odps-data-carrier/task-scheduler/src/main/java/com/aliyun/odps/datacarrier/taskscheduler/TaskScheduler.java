package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metaprocessor.report.ReportBuilder;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskScheduler {

  private static final Logger LOG = LoggerFactory.getLogger(TaskScheduler.class);

  private static final int HEARTBEAT_INTERVAL_MS = 3000;
  private static final int CREATE_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int CREATE_EXTERNAL_TABLE_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int ADD_EXTERNAL_TABLE_PARTITION_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int LOAD_DATA_CONCURRENCY_THRESHOLD_DEFAULT = 10;
  private static final int VALIDATE_CONCURRENCY_THRESHOLD_DEFAULT = 10;

  private static final String INPUT_DIR = "input-dir";
  private static final String DATA_SOURCE = "datasource";
  private static final String MODE = "mode";
  private static final String JDBC_ADDRESS = "jdbc-address";
  private static final String USER = "user";
  private static final String PASSWORD = "password";


  private static final String HELP = "help";

  private ReportBuilder reportBuilder;
  private TaskManager taskManager;
  private DataValidator dataValidator;
  private Map<Action, ActionScheduleInfo> actionScheduleInfoMap;
  private SortedSet<Action> actions;
  private DataSource dataSource;

  private final SchedulerHeartbeatThread heartbeatThread;
  private volatile boolean keepRunning;
  private volatile Throwable savedException;
  protected final AtomicInteger heartbeatIntervalMs;
  protected List<Task> tasks;

  public TaskScheduler() {
    this.heartbeatThread = new SchedulerHeartbeatThread();
    this.keepRunning = true;
    this.reportBuilder = new ReportBuilder();
    this.dataValidator = new DataValidator();
    this.actionScheduleInfoMap = new ConcurrentHashMap<>();
    this.actions = new TreeSet<>(new ActionComparator());
    this.heartbeatIntervalMs = new AtomicInteger(HEARTBEAT_INTERVAL_MS);
    this.tasks = new LinkedList<>();
  }

  private void run(String inputPath, DataSource dataSource, Mode mode, String jdbcAddress, String user, String password) {
    this.dataSource = dataSource;
    generateActions(dataSource);
    this.taskManager = new ScriptTaskManager(inputPath, actions, mode, jdbcAddress, user, password);
    tasks.addAll(this.taskManager.generateTasks(actions, mode));
    //Add data validator
    if (DataSource.Hive.equals(dataSource)) {
      this.dataValidator.generateValidateActions(tasks);
    }
    updateConcurrencyThreshold();
    heartbeatThread.start();
  }

  private void generateActions(DataSource dataSource) {
    if (DataSource.Hive.equals(dataSource)) {
      actions.add(Action.ODPS_CREATE_TABLE);
      actions.add(Action.ODPS_ADD_PARTITION);
      actions.add(Action.HIVE_LOAD_DATA);
      actions.add(Action.HIVE_VALIDATE);
      actions.add(Action.ODPS_VALIDATE);
      actions.add(Action.VALIDATION);
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
            taskManager.shutdown();
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
      if (DataSource.Hive.equals(dataSource)) {
        LOG.info("Start validating data...");
        boolean result = dataValidator.validateAllTasksCountResult(tasks);
        LOG.info("Validate: [{}]", result ? "PASS" : "FAIL");
      }
    }
  }

  private boolean isAllTasksFinished() {
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
      if (Action.VALIDATION.equals(action)) {
        if (dataValidator.validateTaskCountResult(task)) {
          task.changeActionProgress(action, Progress.SUCCEEDED);
        } else {
          task.changeActionProgress(action, Progress.FAILED);
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
          taskManager.getTaskRunner(CommonUtils.getRunnerTypeByAction(action))
              .submitExecutionTask(task, action, executionTaskName);
          actionScheduleInfo.concurrency++;
        }
      }
    }
  }


  private static class ActionComparator implements Comparator<Action> {
    @Override
    public int compare(Action o1, Action o2) {
      return o1.ordinal() - o2.ordinal();
    }
  }

  public static void main(String[] args) throws Exception {
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
        .addOption(jdbcAddress)
        .addOption(user)
        .addOption(password)
        .addOption(help);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(INPUT_DIR)
        && cmd.hasOption(DATA_SOURCE)
        && cmd.hasOption(MODE)
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
      scheduler.run(cmd.getOptionValue(INPUT_DIR), cmdDataSource, cmdMode, cmdJdbcAddress, cmdUser, cmdPassword);
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
