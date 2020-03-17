package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OdpsRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(OdpsRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static final long TOKEN_EXPIRE_INTERVAL = 7 * 24; // hours
  private static final List<String> EMPTY_LIST = Collections.emptyList();

  private static Odps odps;

  public OdpsRunner(MetaConfiguration.OdpsConfiguration odpsConfiguration) {
    if (odpsConfiguration == null) {
      throw new IllegalArgumentException("'odpsConfiguration' cannot be null");
    }

    AliyunAccount account = new AliyunAccount(odpsConfiguration.getAccessId(),
                                              odpsConfiguration.getAccessKey());
    odps = new Odps(account);
    odps.setEndpoint(odpsConfiguration.getEndpoint());
    odps.setDefaultProject(odpsConfiguration.getProjectName());
    LOG.info("Create OdpsRunner succeeded");
  }

  public static class OdpsSqlExecutor implements Runnable {
    private List<String> sqls;
    private Task task;
    private Action action;

    OdpsSqlExecutor(List<String> sqls, Task task, Action action) {
      this.sqls = sqls;
      this.task = task;
      this.action = action;
    }

    @Override
    public void run() {
      Instance i;
      OdpsExecutionInfo odpsExecutionInfo = (OdpsExecutionInfo) task.actionInfoMap.get(action);
      for (String sql : sqls) {
        // Submit
        try {
          Map<String, String> hints = new HashMap<>();
          hints.put("odps.sql.type.system.odps2", "true");
          hints.put("odps.sql.allow.fullscan", "true");
          i = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
        } catch (OdpsException e) {
          LOG.error("Submit ODPS Sql failed, task: " + task +
                    ", project: " + odps.getDefaultProject() +
                    ", sql: \n" + sql +
                    ", exception: " + e.toString());
          task.updateActionExecutionProgress(action, Progress.FAILED);
          return;
        } catch (RuntimeException e) {
          LOG.error("Submit ODPS Sql failed, task: " + task +
                    ", project: " + odps.getDefaultProject() +
                    ", sql: \n" + sql +
                    ", exception: " + e.getMessage());
          e.printStackTrace();
          task.updateActionExecutionProgress(action, Progress.FAILED);
          return;
        }

        // Wait for success
        try {
          i.waitForSuccess();
        } catch (OdpsException e) {
          LOG.error("Run ODPS Sql failed, task: " + task +
                    ", sql: \n" + sql +
                    ", exception: " + e.toString());
          task.updateActionExecutionProgress(action, Progress.FAILED);
          return;
        }

        // Update execution info
        OdpsExecutionInfo.OdpsSqlExecutionInfo info = new OdpsExecutionInfo.OdpsSqlExecutionInfo();
        String instanceId = i.getId();
        info.setInstanceId(instanceId);

        try {
          info.setResult(SQLTask.getResult(i));
        } catch (OdpsException e) {
          LOG.error("Get ODPS Sql result failed, task: " + task +
              ", sql: \n" + sql +
              ", exception: " + e.toString());
        }

        try {
          String logView = i.getOdps().logview().generateLogView(i, TOKEN_EXPIRE_INTERVAL);
          info.setLogView(logView);
        } catch (OdpsException e) {
          LOG.error("Generate ODPS Sql logview failed, task: {}, "
                    + "instance id: {}, exception: {}", task, instanceId,
                    ExceptionUtils.getStackTrace(e));
        }

        odpsExecutionInfo.addInfo(info);
        LOG.debug("Task: {}, {}", task, odpsExecutionInfo.getOdpsExecutionInfoSummary());
        RUNNER_LOG.info("Task: {}, Action: {} {}",
            task, action, odpsExecutionInfo.getOdpsExecutionInfoSummary());
      }
      task.updateActionExecutionProgress(action, Progress.SUCCEEDED);
    }
  }

  private List<String> getSqlStatements(Task task, Action action) {
    List<String> sqlStatements = new LinkedList<>();
    switch (action) {
      case ODPS_CREATE_TABLE:
        if (task.tableMetaModel.partitionColumns.isEmpty()) {
          //Non-partition table should drop table at first.
          sqlStatements.add(OdpsSqlUtils.getDropTableStatement(task.tableMetaModel));
        }
        sqlStatements.add(OdpsSqlUtils.getCreateTableStatement(task.tableMetaModel));
        return sqlStatements;
      case ODPS_ADD_PARTITION:
        String dropPartitionStatement = OdpsSqlUtils.getDropPartitionStatement(task.tableMetaModel);
        if (!StringUtils.isNullOrEmpty(dropPartitionStatement)) {
          sqlStatements.add(dropPartitionStatement);
        }
        String addPartitionStatement = OdpsSqlUtils.getAddPartitionStatement(task.tableMetaModel);
        if (!StringUtils.isNullOrEmpty(addPartitionStatement)) {
          sqlStatements.add(addPartitionStatement);
        }
        return sqlStatements;
      case ODPS_VALIDATE:
        sqlStatements.add(OdpsSqlUtils.getVerifySql(task.tableMetaModel));
        return sqlStatements;
    }
    return EMPTY_LIST;
  }

  @Override
  public void submitExecutionTask(Task task, Action action) {
    List<String> sqlStatements = getSqlStatements(task, action);
    LOG.info("SQL Statements: {}", String.join(", ", sqlStatements));
    if (sqlStatements.isEmpty()) {
      task.updateActionExecutionProgress(action, Progress.SUCCEEDED);
      LOG.error("Empty sqlStatement, mark done, action: {}", action);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    LOG.info("Submit {} task: {}, action: {}, taskProgress: {}",
        runnerType.name(), task, action.name(), task.progress);

    this.runnerPool.execute(new OdpsSqlExecutor(sqlStatements,
        task,
        action));
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
