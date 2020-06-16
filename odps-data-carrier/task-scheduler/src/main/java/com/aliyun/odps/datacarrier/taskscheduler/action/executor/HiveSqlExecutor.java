package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hive.jdbc.HiveStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.HiveSqlActionInfo;
import com.aliyun.odps.utils.StringUtils;

public class HiveSqlExecutor extends AbstractActionExecutor {

  private static final Logger LOG = LogManager.getLogger(HiveSqlExecutor.class);

  public HiveSqlExecutor() {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Create HiveRunner failed", e);
    }
  }

  private static class HiveSqlCallable implements Callable<List<List<String>>> {

    private String hiveJdbcUrl;
    private String user;
    private String password;
    private String sql;
    // TODO: Map<String, String> could be better
    private List<String> settings;
    private String actionId;
    private HiveSqlActionInfo hiveSqlActionInfo;

    HiveSqlCallable(
        String hiveJdbcUrl,
        String user,
        String password,
        String sql,
        List<String> settings,
        String actionId,
        HiveSqlActionInfo hiveSqlActionInfo) {
      this.hiveJdbcUrl = Objects.requireNonNull(hiveJdbcUrl);
      this.user = Objects.requireNonNull(user);
      this.password = Objects.requireNonNull(password);
      this.sql = Objects.requireNonNull(sql);
      this.settings = Objects.requireNonNull(settings);
      this.actionId = Objects.requireNonNull(actionId);
      this.hiveSqlActionInfo = Objects.requireNonNull(hiveSqlActionInfo);
    }

    @Override
    public List<List<String>> call() throws SQLException {
      LOG.info("Executing sql: {}", sql);

      try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, user, password)) {
        try (HiveStatement stmt = (HiveStatement) conn.createStatement()) {
          for (String setting : settings) {
            stmt.execute("SET " + setting);
          }

          Runnable logging = () -> {
            while (stmt.hasMoreLogs()) {
              try {
                for (String line : stmt.getQueryLog()) {
                  LOG.info("Hive >>> {}", line);
                  parseLogAndSetExecutionInfo(line, actionId, hiveSqlActionInfo);
                }
              } catch (SQLException e) {
                LOG.warn("Fetching hive query log failed", e);
                break;
              }
            }
          };
          Thread loggingThread = new Thread(logging);
          loggingThread.start();

          List<List<String>> ret = new LinkedList<>();
          try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            while (rs.next()) {
              List<String> record = new LinkedList<>();
              for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                record.add(rs.getString(i));
              }

              ret.add(record);
            }
          }

          try {
            loggingThread.join();
          } catch (InterruptedException ignore) {
          }

          return ret;
        }
      }
    }
  }

  public Future<List<List<String>>> execute(
      String sql,
      String actionId,
      HiveSqlActionInfo hiveSqlActionInfo) {

    // TODO: jdbc address, user, password should come with tableMigrationConfig
    // TODO: different action could have different settings
    // TODO: make settings a map
    HiveSqlCallable callable = new HiveSqlCallable(
        MmaServerConfig.getInstance().getHiveConfig().getJdbcConnectionUrl(),
        MmaServerConfig.getInstance().getHiveConfig().getUser(),
        MmaServerConfig.getInstance().getHiveConfig().getPassword(),
        sql,
        MmaServerConfig.getInstance().getHiveConfig().getHiveJdbcExtraSettings(),
        actionId,
        hiveSqlActionInfo);

    return executor.submit(callable);
  }

  private static void parseLogAndSetExecutionInfo(
      String log,
      String actionId,
      HiveSqlActionInfo hiveSqlActionInfo) {

    if (StringUtils.isNullOrEmpty(log)) {
      return;
    }
    if (!log.contains("Starting Job =")) {
      return;
    }
    String jobId = log.split("=")[1].split(",")[0];
    String trackingUrl = log.split("=")[2];
    hiveSqlActionInfo.setJobId(jobId);
    hiveSqlActionInfo.setTrackingUrl(trackingUrl);
    LOG.info("JobId: {}, actionId: {}", jobId, actionId);
  }
}
