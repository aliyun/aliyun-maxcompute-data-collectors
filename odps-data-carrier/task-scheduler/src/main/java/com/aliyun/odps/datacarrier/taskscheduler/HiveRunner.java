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
        if (task != null) {
          task.updateExecutionProgress(action, executionTaskName, Progress.FAILED);
        }
        return;
      }

      if (task != null) {
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
