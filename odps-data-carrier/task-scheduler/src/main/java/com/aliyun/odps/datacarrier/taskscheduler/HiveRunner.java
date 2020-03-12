package com.aliyun.odps.datacarrier.taskscheduler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.utils.StringUtils;



public class HiveRunner extends AbstractTaskRunner {

  private static final Logger LOG = LogManager.getLogger(HiveRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static String jdbcAddress;
  private static String user;
  private static String password;
  private static List<String> extraSettings = new LinkedList<>();

  public HiveRunner(MetaConfiguration.HiveConfiguration hiveConfiguration) {
    if (hiveConfiguration == null) {
      throw new IllegalArgumentException("'hiveConfiguration' cannot be null");
    }

    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Create HiveRunner failed");
    }

    jdbcAddress = hiveConfiguration.getHiveJdbcAddress();
    user = hiveConfiguration.getUser();
    password = hiveConfiguration.getPassword();
    extraSettings = hiveConfiguration.getHiveJdbcExtraSettings();
  }

  public static class HiveSqlExecutor implements Runnable {
    private List<String> sqls;
    private Task task;
    private Action action;
    private String executionTaskName;

    HiveSqlExecutor(List<String> sqls, Task task, Action action, String executionTaskName) {
      this.sqls = sqls;
      this.task = task;
      this.action = action;
      this.executionTaskName = executionTaskName;
    }

    @Override
    public void run() {
      try {
        LOG.info("HiveSqlExecutor execute task {}, {}, {}, {}",
            task,
            action,
            executionTaskName,
            String.join(", ", sqls));

        Connection con = DriverManager.getConnection(jdbcAddress, user, password);

        // Apply JDBC extra settings
        HiveStatement settingsStatement = (HiveStatement) con.createStatement();
        for (String setting : extraSettings) {
          settingsStatement.execute("SET " + setting);
        }
        settingsStatement.close();

        HiveStatement statement = (HiveStatement) con.createStatement();
        HiveExecutionInfo hiveExecutionInfo = (HiveExecutionInfo) task.actionInfoMap
            .get(action).executionInfoMap.get(executionTaskName);

        Runnable logging = () -> {
          while (statement.hasMoreLogs()) {
            try {
              for (String line : statement.getQueryLog()) {
                LOG.info("Hive >>> {}", line);
                // TODO: execution info is overwrote
                parseLogSetExecutionInfo(line, hiveExecutionInfo);
              }
            } catch (SQLException e) {
              LOG.warn("Fetching hive query log failed");
            }
          }
        };
        Thread loggingThread = new Thread(logging);
        loggingThread.start();

        for (String sql : sqls) {
          ResultSet resultSet = statement.executeQuery(sql);
          resultSet.close();
        }
        LOG.debug("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());
        RUNNER_LOG.info("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());

        try {
          loggingThread.join();
        } catch (InterruptedException e) {
          // Ignore
        }
        statement.close();
        con.close();
      } catch (SQLException e) {
        LOG.error("Run HIVE Sql failed, " +
                  "sql: \n" + String.join(", ", sqls) +
                  "\nexception: " + ExceptionUtils.getStackTrace(e));
        if (task != null) {
          LOG.info("Hive SQL FAILED {}, {}, {}", action, executionTaskName, task.toString());
          task.updateExecutionProgress(action, executionTaskName, Progress.FAILED);
        }
        return;
      }

      if (task != null) {
        LOG.info("Hive SQL SUCCEEDED {}, {}, {}", action, executionTaskName, task.toString());
        task.updateExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
      }
    }
  }

  private static void parseLogSetExecutionInfo(String log, HiveExecutionInfo hiveExecutionInfo) {
    if (StringUtils.isNullOrEmpty(log) || hiveExecutionInfo == null) {
      return;
    }
    if (!log.contains("Starting Job =")) {
      return;
    }
    String jobId = log.split("=")[1].split(",")[0];
    String trackingUrl = log.split("=")[2];
    hiveExecutionInfo.setJobId(jobId);
    hiveExecutionInfo.setTrackingUrl(trackingUrl);
  }

  public void shutdown() {
    super.shutdown();
  }
}
