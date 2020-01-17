package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OdpsRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(OdpsRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  public static final String PROJECT_NAME = "project_name";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";
  public static final String END_POINT = "end_point";

  private static final long TOKEN_EXPIRE_INTERVAL = 7 * 24; // hours
  private static final String ODPS_CONFIG_INI = "odps_config.ini";

  private static Properties properties;
  private static Odps odps;

  public OdpsRunner() {
    try {
      this.odps = odps();
      LOG.info("Create OdpsRunner succeeded.");
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Create odps failed.");
    }
  }

  public static Odps odps() throws IOException {
    String odpsConfigPath = System.getProperty("user.dir") + "/odps-data-carrier/" + ODPS_CONFIG_INI;
    LOG.info("odpsConfigPath = {}", odpsConfigPath);
    InputStream is = new FileInputStream(odpsConfigPath);
    properties = new Properties();
    properties.load(is);
    AliyunAccount account = new AliyunAccount(properties.getProperty(ACCESS_ID),
        properties.getProperty(ACCESS_KEY));
    Odps odps = new Odps(account);
    if (properties.getProperty(PROJECT_NAME) == null) {
      throw new IllegalArgumentException("Project should not be empty.");
    }
    odps.setDefaultProject(properties.getProperty(PROJECT_NAME));
    if (properties.getProperty(END_POINT) == null) {
      throw new IllegalArgumentException("Endpoint should not be empty.");
    }
    odps.setEndpoint(properties.getProperty(END_POINT));
    return odps;
  }

  public static class OdpsSqlExecutor implements Runnable {
    private String sql;
    private Task task;
    private Action action;
    private String executionTaskName;

    OdpsSqlExecutor(String sql, Task task, Action action, String executionTaskName) {
      this.sql = sql;
      this.task = task;
      this.action = action;
      this.executionTaskName = executionTaskName;
    }

    @Override
    public void run() {
      Instance i;
      try {
        //validate sql statement, multi-statement query will set odps.sql.submit.mode=script
        Map<String, String> hints = new HashMap<>();
        if (sql.chars().filter(ch -> ch == ';').count() > 1) {
          LOG.info("multi-statement query, will \"SET odps.sql.submit.mode=script\"", sql);
          hints.put("odps.sql.submit.mode", "script");
        }
        hints.put("odps.sql.type.system.odps2", "true");
        hints.put("odps.sql.allow.fullscan", "true");
        i = SQLTask.run(odps, properties.getProperty(PROJECT_NAME), sql, hints, null);
      } catch (OdpsException e) {
        LOG.error("Submit ODPS Sql failed, task: " + task +
            ", executionTaskName: " + executionTaskName +
            ", project: " + properties.getProperty(PROJECT_NAME) +
            ", sql: \n" + sql +
            ", exception: " + e.toString());
        task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
        return;
      } catch (RuntimeException e) {
        LOG.error("Submit ODPS Sql failed, task: " + task +
            ", executionTaskName: " + executionTaskName +
            ", project: " + properties.getProperty(PROJECT_NAME) +
            ", sql: \n" + sql +
            ", exception: " + e.getMessage());
        e.printStackTrace();
        task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
        return;
      }

      try {
        i.waitForSuccess();
      } catch (OdpsException e) {
        LOG.error("Run ODPS Sql failed, task: " + task +
            ", executionTaskName: " + executionTaskName +
            ", sql: \n" + sql +
            ", exception: " + e.toString());
        task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
        return;
      }
      OdpsExecutionInfo odpsExecutionInfo =
          (OdpsExecutionInfo) task.actionInfoMap.get(action).executionInfoMap.get(executionTaskName);
      String instanceId = i.getId();
      odpsExecutionInfo.setInstanceId(instanceId);
      String logView = "";
      try {
        logView = i.getOdps().logview().generateLogView(i, TOKEN_EXPIRE_INTERVAL);
        odpsExecutionInfo.setLogView(logView);
      } catch (OdpsException e) {
        LOG.error("Generate ODPS Sql logview failed, task: " + task +
            "executionTaskName: " + executionTaskName +
            "exception: " + e.toString());
      }
      LOG.debug("Task: {}, {}", task, odpsExecutionInfo.getOdpsExecutionInfoSummary());
      RUNNER_LOG.info("Task: {}, {}", task, odpsExecutionInfo.getOdpsExecutionInfoSummary());
      // need result when sql is node script mode.
      if (!odpsExecutionInfo.isScriptMode()) {
        try {
          List<Record> records = SQLTask.getResult(i);
          // hacky: get result for count(1), only 1 record and 1 column.
          String result = records.get(0).get(0).toString();
          odpsExecutionInfo.setResult(result);
        } catch (OdpsException e) {
          LOG.error("Get ODPS Sql result failed, task: " + task +
              ", executionTaskName: " + executionTaskName +
              ", sql: \n" + sql +
              ", exception: " + e.toString());
        }
      }
      task.changeExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
