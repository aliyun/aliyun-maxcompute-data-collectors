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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hive.jdbc.HiveStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class HiveRunner extends AbstractTaskRunner {

  private static final Logger LOG = LogManager.getLogger(HiveRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
  private static String EXTRA_SETTINGS_INI = "extra_settings_jdbc.ini";

  private static String jdbcAddress;
  private static String user;
  private static String password;
  private static List<String> extraSettings = new LinkedList<>();

  public HiveRunner(MetaConfiguration.HiveConfiguration hiveConfiguration) {
    if (hiveConfiguration == null) {
      throw new IllegalArgumentException("'hiveConfiguration' cannot be null");
    }

    try{
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Create HiveRunner failed");
    }

    jdbcAddress = hiveConfiguration.getHiveJdbcAddress();
    user = hiveConfiguration.getUser();
    password = hiveConfiguration.getPassword();

    loadExtraSettings();
  }

  private void loadExtraSettings() {
    // TODO: use a fixed parent directory
    Path extraSettingsPath = Paths.get(System.getProperty("user.dir"),
                                       EXTRA_SETTINGS_INI);
    LOG.info("JDBC extra settings path: {}", extraSettingsPath);
    if (!extraSettingsPath.toFile().exists()) {
      LOG.warn("JDBC extra settings path does not exist");
      return;
    }
    try {
      List<String> settings = Files.readAllLines(extraSettingsPath);
      for (String setting : settings) {
        if (setting.trim().isEmpty() || setting.trim().startsWith("#")) {
          continue;
        }
        extraSettings.add(setting.trim());
        LOG.info("Load setting: {}", setting);
      }
    } catch (IOException e) {
      LOG.error("Load extra settings failed, {}", e.getMessage());
      e.printStackTrace();
    }
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
        LOG.info("HiveSqlExecutor execute task {}, {}, {}",
                 task,
                 action,
                 String.join(", ", sqls));

        Connection con = DriverManager.getConnection(jdbcAddress, user, password);

        // Apply JDBC extra settings
        Statement settingsStatement = con.createStatement();
        for (String setting : extraSettings) {
          settingsStatement.execute("SET " + setting);
        }
        settingsStatement.close();

        Statement statement = con.createStatement();
        HiveStatement hiveStatement = (HiveStatement) statement;
        HiveExecutionInfo hiveExecutionInfo = (HiveExecutionInfo) task.actionInfoMap
            .get(action).executionInfoMap.get(executionTaskName);
        //statement.setQueryTimeout(24 * 60 * 60);
        for (String sql : sqls) {
          ResultSet resultSet = statement.executeQuery(sql);
          while (resultSet.next()) {
            // -- getQueryLog --
            try {
              List<String> queryLogs = hiveStatement.getQueryLog();
              for (String log : queryLogs) {
                LOG.info("Hive --> " + log);
                // TODO: execution info is overwrote
                parseLogSetExecutionInfo(log, hiveExecutionInfo);
              }
            } catch (SQLException e) {
              LOG.error("Query log failed. ");
            }
//          // -- getQueryLog --
//          if (Action.VALIDATION_BY_TABLE.equals(action)) {
//            LOG.info("executeQuery result: {}", resultSet.getString(1));
//            hiveExecutionInfo.setResult(resultSet.getString(1));
//          } else if (Action.VALIDATION_BY_PARTITION.equals(action)) {
//            LOG.info("executeQuery result: {}", resultSet.getString(1));
//            //TODO[mingyou] need to support multiple partition key.
//            List<String> partitionValues = Lists.newArrayList(resultSet.getString(1).split("\n"));
//            List<String> counts = Lists.newArrayList(resultSet.getString(2).split("\n"));
//            if(partitionValues.size() != counts.size()) {
//              hiveExecutionInfo.setMultiRecordResult(Collections.emptyMap());
//            } else {
//              Map<String, String> result = new HashMap<>();
//              for (int i = 0; i < partitionValues.size(); i++) {
//                result.put(partitionValues.get(i), counts.get(i));
//              }
//              hiveExecutionInfo.setMultiRecordResult(result);
//            }
//          }
            break;
          }
          resultSet.close();
        }
        LOG.debug("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());
        RUNNER_LOG.info("Task: {}, {}", task, hiveExecutionInfo.getHiveExecutionInfoSummary());
        statement.close();
        con.close();
      } catch (SQLException e) {
        LOG.error("Run HIVE Sql failed, " +
                  "sql: \n" + String.join(", ", sqls) +
                  "\nexception: " + ExceptionUtils.getStackTrace(e));
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
