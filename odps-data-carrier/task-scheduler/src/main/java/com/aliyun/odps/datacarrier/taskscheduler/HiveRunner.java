package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import org.apache.hive.jdbc.HiveStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class HiveRunner extends AbstractTaskRunner {

  private static final Logger LOG = LogManager.getLogger(HiveRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static String EXTRA_SETTINGS_INI = "extra_settings_jdbc.ini";

//  private static Connection con;

  private static String jdbcAddress;
  private static String user;
  private static String password;

  public HiveRunner(String jdbcAddress, String user, String password) {
    try{
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Create HiveRunner failed.");
    }

    this.jdbcAddress = jdbcAddress;
    this.user = user;
    this.password = password;

//    try {
//      con = DriverManager.getConnection(jdbcAddress, user, password);
//      LOG.info("Create HiveRunner succeeded.");
//    } catch (SQLException e) {
//      e.printStackTrace();
//      throw new RuntimeException("Create connection to JDBC failed. ");
//    }

    loadExtraSettings();
  }

  private void loadExtraSettings() {
    Path extraSettingsPath = Paths.get(System.getProperty("user.dir"), "odps-data-carrier", EXTRA_SETTINGS_INI);
    LOG.info("extraSettingsPath = {}", extraSettingsPath);
    try {
      List<String> settings = Files.readAllLines(extraSettingsPath);
      for (String setting : settings) {
        if (setting.startsWith("#")) {
          continue;
        }
        String settingSql = "set " + setting.trim();
        LOG.info("Load setting: {}", settingSql);
        asyncExecuteSql(settingSql);
      }
    } catch (IOException e) {
      LOG.error("Load extra settings failed, {}", e.getMessage());
      e.printStackTrace();
    }
  }

  private void asyncExecuteSql(String sqlStr) {
    this.runnerPool.execute(new HiveSqlExecutor(sqlStr));
  }

  public static class HiveSqlExecutor implements Runnable {
    private String sql;
    private Task task;
    private Action action;
    private String executionTaskName;

    HiveSqlExecutor(String sql) {
      this(sql, null, null, null);
    }

    HiveSqlExecutor(String sql, Task task, Action action, String executionTaskName) {
      this.sql = sql;
      this.task = task;
      this.action = action;
      this.executionTaskName = executionTaskName;
    }

    @Override
    public void run() {
      try {
        LOG.info("HiveSqlExecutor execute task {}, {}, {}", task, action, sql);
        Connection con = DriverManager.getConnection(jdbcAddress, user, password);
        Statement statement = con.createStatement();
        HiveStatement hiveStatement = (HiveStatement) statement;
        HiveExecutionInfo hiveExecutionInfo = null;
        if (task != null) {
         hiveExecutionInfo = (HiveExecutionInfo) task.actionInfoMap.get(action).executionInfoMap.get(executionTaskName);
        }
        //statement.setQueryTimeout(24 * 60 * 60);
        if (hiveExecutionInfo != null) {
          ResultSet resultSet = statement.executeQuery(sql);
          while(resultSet.next()) {
            // -- getQueryLog --
            try {
              List<String> queryLogs = hiveStatement.getQueryLog();
              for (String log : queryLogs) {
                LOG.info("Hive-->" + log);
                parseLogSetExecutionInfo(log, hiveExecutionInfo);
              }
            } catch (SQLException e) {
              LOG.error("Query log failed. ");
            }
            // -- getQueryLog --
            LOG.info("executeQuery result: {}", resultSet.getString(1));
            hiveExecutionInfo.setResult(resultSet.getString(1));
            break;
          }
          resultSet.close();
          LOG.debug("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());
          RUNNER_LOG.info("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());
        } else {
          // for load settings.
          statement.execute(sql);
        }
        statement.close();
        con.close();
      } catch (SQLException e) {
        LOG.error("Run HIVE Sql failed, " +
            "sql: \n" + sql +
            "\nexception " + e.toString());
        e.printStackTrace();
        if (task != null) {
          task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
        }
        return;
      }

      if (task != null) {
        task.changeExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
      }
    }
  }

  private static void parseLogSetExecutionInfo(String log, HiveExecutionInfo hiveExecutionInfo) {
    if (StringUtils.isNullOrEmpty(log) || hiveExecutionInfo == null) {
      return;
    }
    if (log.indexOf("Starting Job =") == -1) {
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
