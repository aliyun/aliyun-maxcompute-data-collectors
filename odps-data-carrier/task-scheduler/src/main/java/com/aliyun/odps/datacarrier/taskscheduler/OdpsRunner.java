package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OdpsRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(OdpsRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static final long TOKEN_EXPIRE_INTERVAL = 7 * 24; // hours
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
    private String executionTaskName;

    OdpsSqlExecutor(List<String> sqls, Task task, Action action, String executionTaskName) {
      this.sqls = sqls;
      this.task = task;
      this.action = action;
      this.executionTaskName = executionTaskName;
    }

    @Override
    public void run() {
      Instance i;

      for (String sql : sqls) {
        // Submit
        try {
          Map<String, String> hints = new HashMap<>();
          hints.put("odps.sql.type.system.odps2", "true");
          hints.put("odps.sql.allow.fullscan", "true");
          i = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
        } catch (OdpsException e) {
          LOG.error("Submit ODPS Sql failed, task: " + task +
                    ", executionTaskName: " + executionTaskName +
                    ", project: " + odps.getDefaultProject() +
                    ", sql: \n" + sql +
                    ", exception: " + e.toString());
          task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
          return;
        } catch (RuntimeException e) {
          LOG.error("Submit ODPS Sql failed, task: " + task +
                    ", executionTaskName: " + executionTaskName +
                    ", project: " + odps.getDefaultProject() +
                    ", sql: \n" + sql +
                    ", exception: " + e.getMessage());
          e.printStackTrace();
          task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
          return;
        }

        // Wait for success
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

        // Update execution info
        // TODO: execution info is overwrote
        OdpsExecutionInfo odpsExecutionInfo =
            (OdpsExecutionInfo) task.actionInfoMap.get(action).executionInfoMap
                .get(executionTaskName);
        String instanceId = i.getId();
        odpsExecutionInfo.setInstanceId(instanceId);
        try {
          String logView = i.getOdps().logview().generateLogView(i, TOKEN_EXPIRE_INTERVAL);
          odpsExecutionInfo.setLogView(logView);
        } catch (OdpsException e) {
          LOG.error("Generate ODPS Sql logview failed, task: {}, executionTaskName: {}, "
                    + "instance id: {}, exception: {}", task, executionTaskName, instanceId,
                    ExceptionUtils.getStackTrace(e));
        }

        LOG.debug("Task: {}, {}", task, odpsExecutionInfo.getOdpsExecutionInfoSummary());
        RUNNER_LOG.info("Task: {}, {}", task, odpsExecutionInfo.getOdpsExecutionInfoSummary());
      }

      // need result when sql is node script mode.
//      if (!odpsExecutionInfo.isScriptMode()) {
//        try {
//          List<Record> records = SQLTask.getResult(i);
//          if (Action.VALIDATION_BY_TABLE.equals(action)) {
//            // hacky: get result for count(1), only 1 record and 1 column.
//            String result = records.get(0).get(0).toString();
//            odpsExecutionInfo.setResult(result);
//          } else if (Action.VALIDATION_BY_PARTITION.equals(action)) {
//            //TODO[mingyou] need to support multiple partition key.
//            List<String> partitionValues = Lists.newArrayList(records.get(0).toString().split("\n"));
//            List<String> counts = Lists.newArrayList(records.get(1).toString().split("\n"));
//            if(partitionValues.size() != counts.size()) {
//              odpsExecutionInfo.setMultiRecordResult(Collections.emptyMap());
//            } else {
//              Map<String, String> result = new HashMap<>();
//              for (int index = 0; index < partitionValues.size(); index++) {
//                result.put(partitionValues.get(index), counts.get(index));
//              }
//              odpsExecutionInfo.setMultiRecordResult(result);
//            }
//          }
//        } catch (OdpsException e) {
//          LOG.error("Get ODPS Sql result failed, task: " + task +
//              ", executionTaskName: " + executionTaskName +
//              ", sql: \n" + sql +
//              ", exception: " + e.toString());
//        }
//      }
      task.changeExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
    }
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
