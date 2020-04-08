/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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

  public HiveRunner(MmaConfig.HiveConfig hiveConfiguration) {
    if (hiveConfiguration == null) {
      throw new IllegalArgumentException("'hiveConfiguration' cannot be null");
    }

    try {
      Class.forName(DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException("Create HiveRunner failed");
    }

    jdbcAddress = hiveConfiguration.getJdbcConnectionUrl();
    user = hiveConfiguration.getUser();
    password = hiveConfiguration.getPassword();
    extraSettings = hiveConfiguration.getHiveJdbcExtraSettings();
  }

  public static class HiveSqlExecutor implements Runnable {
    private String sql;
    private Task task;
    private Action action;

    HiveSqlExecutor(String sql, Task task, Action action) {
      this.sql = sql;
      this.task = task;
      this.action = action;
    }

    @Override
    public void run() {
      try {
        LOG.info("HiveSqlExecutor execute task {}, {}, {}", task, action, sql);

        Connection con = DriverManager.getConnection(jdbcAddress, user, password);

        // Apply JDBC extra settings
        HiveStatement settingsStatement = (HiveStatement) con.createStatement();
        for (String setting : extraSettings) {
          settingsStatement.execute("SET " + setting);
        }
        settingsStatement.close();

        HiveActionInfo hiveExecutionInfo = (HiveActionInfo) task.actionInfoMap.get(action);
        HiveStatement statement = (HiveStatement) con.createStatement();
        Runnable logging = () -> {
          while (statement.hasMoreLogs()) {
            try {
              for (String line : statement.getQueryLog()) {
                LOG.info("Hive >>> {}", line);
                parseLogSetExecutionInfo(line, hiveExecutionInfo);
              }
            } catch (SQLException e) {
              LOG.warn("Fetching hive query log failed", e);
              break;
            }
          }
        };
        Thread loggingThread = new Thread(logging);
        loggingThread.start();

        ResultSet resultSet = statement.executeQuery(sql);

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnsNumber = resultSetMetaData.getColumnCount();
        LOG.debug("task: {}, action: {}, columnsNumber: {}", task.getName(), action, columnsNumber);
        List<List<String>> result = new LinkedList<>();
        while (resultSet.next()) {
          List<String> record = new LinkedList<>();
          for (int i = 1; i <= columnsNumber; i++) {
            record.add(resultSet.getString(i));
          }
          result.add(record);
        }
        hiveExecutionInfo.setResult(result);
        resultSet.close();

        LOG.debug("Task: {}, {}", task, hiveExecutionInfo.getHiveActionInfoSummary());
        RUNNER_LOG.info("Task: {}, Action: {} {}",
            task, action, hiveExecutionInfo.getHiveActionInfoSummary());
        try {
          loggingThread.join();
        } catch (InterruptedException e) {
          // Ignore
        }
        statement.close();
        con.close();
      } catch (SQLException e) {
        LOG.error("Run HIVE Sql failed, {}, \nexception: {}", sql, ExceptionUtils.getStackTrace(e));
        if (task != null) {
          LOG.info("Hive SQL FAILED {}, {}", action, task.toString());
          try {
            // TODO: should retry
            task.updateActionProgress(action, Progress.FAILED);
          } catch (MmaException ex) {
            LOG.error(ex);
          }
        }
        return;
      }

      if (task != null) {
        LOG.info("Hive SQL SUCCEEDED {}, {}", action, task.toString());
        try {
          // TODO: should retry
          task.updateActionProgress(action, Progress.SUCCEEDED);
        } catch (MmaException e) {
          LOG.error(e);
        }
      }
    }
  }

  private static void parseLogSetExecutionInfo(String log, HiveActionInfo hiveExecutionInfo) {
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

  private String getSqlStatements(Task task, Action action) {
    switch (action) {
      case HIVE_LOAD_DATA:
        return HiveSqlUtils.getUdtfSql(task.tableMetaModel);
      case HIVE_VERIFICATION:
        return HiveSqlUtils.getVerifySql(task.tableMetaModel);
    }
    return "";
  }

  @Override
  public void submitExecutionTask(Task task, Action action) throws MmaException {
    String sqlStatement = getSqlStatements(task, action);
    if (StringUtils.isNullOrEmpty(sqlStatement)) {
      task.updateActionProgress(action, Progress.SUCCEEDED);
      LOG.error("Empty sqlStatement, mark done, action: {}", action);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    LOG.info("Submit {} task: {}, action: {}, taskProgress: {}",
        runnerType.name(), task, action.name(), task.progress);
    this.runnerPool.execute(new HiveSqlExecutor(getSqlStatements(task, action),
        task,
        action));
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
