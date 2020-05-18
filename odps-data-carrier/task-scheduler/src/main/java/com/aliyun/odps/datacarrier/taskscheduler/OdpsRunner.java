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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;

public class OdpsRunner extends AbstractTaskRunner {
  private static final Logger LOG = LogManager.getLogger(OdpsRunner.class);
  private static final Logger RUNNER_LOG = LogManager.getLogger("RunnerLogger");

  private static final long TOKEN_EXPIRE_INTERVAL = 7 * 24; // hours

  private static Odps odps;
  private static MmaConfig.OdpsConfig odpsConfig;
  private static MmaConfig.OssConfig ossConfig; // used to create external tables which are stored in OSS

  public OdpsRunner(MmaConfig.OdpsConfig odpsConfiguration,
                    MmaConfig.OssConfig ossConfiguration) {
    if (odpsConfiguration == null) {
      throw new IllegalArgumentException("'odpsConfiguration' cannot be null");
    }

    odpsConfig = odpsConfiguration;
    ossConfig = ossConfiguration;
    AliyunAccount account = new AliyunAccount(odpsConfiguration.getAccessId(),
                                              odpsConfiguration.getAccessKey());
    odps = new Odps(account);
    odps.setEndpoint(odpsConfiguration.getEndpoint());
    odps.setDefaultProject(odpsConfiguration.getProjectName());
    Map<String, String> globalSettings = odps.getGlobalSettings();
    globalSettings.putAll(odpsConfiguration.getGlobalSettings());
    odps.setGlobalSettings(globalSettings);
    LOG.info("Create OdpsRunner succeeded");
  }

  public static class OdpsSqlExecutor implements Runnable {
    private String sql;
    private Task task;
    private Action action;

    OdpsSqlExecutor(String sql, Task task, Action action) {
      this.sql = sql;
      this.task = task;
      this.action = action;
    }

    @Override
    public void run() {
      Instance i;
      // Submit
      try {
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.sql.type.system.odps2", "true");
        hints.put("odps.sql.allow.fullscan", "true");
        switch (action) {
          case ODPS_DROP_TABLE:
          case ODPS_CREATE_TABLE:
          case ODPS_CREATE_EXTERNAL_TABLE:
          case ODPS_DROP_PARTITION:
          case ODPS_ADD_PARTITION:
          case ODPS_ADD_EXTERNAL_TABLE_PARTITION:
            hints.putAll(odpsConfig.getDestinationTableSettings().getDDLSettings());
            break;
          case ODPS_LOAD_DATA:
            hints.putAll(odpsConfig.getSourceTableSettings().getMigrationSettings());
            break;
          case ODPS_SOURCE_VERIFICATION:
            hints.putAll(odpsConfig.getSourceTableSettings().getVerifySettings());
          case ODPS_DESTINATION_VERIFICATION:
            hints.putAll(odpsConfig.getDestinationTableSettings().getVerifySettings());
          default:
            break;
        }
        i = SQLTask.run(odps, odps.getDefaultProject(), sql, hints, null);
      } catch (OdpsException e) {
        LOG.error("Submit ODPS Sql failed, task: " + task +
                  ", project: " + odps.getDefaultProject() +
                  ", sql: \n" + sql +
                  ", exception: " + e.toString());
        try {
          // TODO: should retry
          task.updateActionProgress(action, Progress.FAILED);
        } catch (MmaException ex) {
          LOG.error(ex);
        }
        return;
      } catch (RuntimeException e) {
        LOG.error("Submit ODPS Sql failed, task: " + task +
                  ", project: " + odps.getDefaultProject() +
                  ", sql: \n" + sql +
                  ", exception: " + e.getMessage());
        e.printStackTrace();
        try {
          // TODO: should retry
          task.updateActionProgress(action, Progress.FAILED);
        } catch (MmaException ex) {
          LOG.error(ex);
        }
        return;
      }

      // Update action info
      OdpsActionInfo odpsActionInfo = (OdpsActionInfo) task.actionInfoMap.get(action);
      String instanceId = i.getId();
      String logviewUrl = null;
      try {
        logviewUrl = i.getOdps().logview().generateLogView(i, TOKEN_EXPIRE_INTERVAL);
      } catch (OdpsException e) {
        LOG.error("Generate ODPS Sql logview failed, task: {}, "
                  + "instance id: {}, exception: {}", task, instanceId,
                  ExceptionUtils.getStackTrace(e));
      }
      odpsActionInfo.setInstanceId(i.getId());
      odpsActionInfo.setLogView(logviewUrl);
      LOG.info("Task: {}, Action: {} {}",
               task, action, odpsActionInfo.getOdpsActionInfoSummary());
      RUNNER_LOG.info("Task: {}, Action: {} {}",
                      task, action, odpsActionInfo.getOdpsActionInfoSummary());

      // Wait for success
      try {
        i.waitForSuccess();
      } catch (OdpsException e) {
        LOG.error("Run ODPS Sql failed, task: " + task +
                  ", sql: \n" + sql +
                  ", exception: " + e.toString());
        try {
          // TODO: should retry
          task.updateActionProgress(action, Progress.FAILED);
        } catch (MmaException ex) {
          LOG.error(ex);
        }
        return;
      }

      try {
        odpsActionInfo.setResult(SQLTask.getResult(i));
      } catch (OdpsException e) {
        LOG.error("Get ODPS Sql result failed, task: " + task +
            ", sql: \n" + sql +
            ", exception: " + e.toString());
      }

      try {
        // TODO: should retry
        task.updateActionProgress(action, Progress.SUCCEEDED);
        LOG.info("Run ODPS Sql succeeded, task: {}, action: {}", task, action);
      } catch (MmaException e) {
        LOG.error(e);
      }
    }
  }

  private String getSqlStatement(Task task, Action action) throws MmaException {
      ExternalTableConfig externalTableConfig = null;
      if (!StringUtils.isNullOrEmpty(task.tableMetaModel.odpsTableStorage)) {
        if (ossConfig != null &&
            ExternalTableStorage.OSS.name().equalsIgnoreCase(task.tableMetaModel.odpsTableStorage)) {
          externalTableConfig = new OssExternalTableConfig(ossConfig.getOssEndpoint(),
                                                           ossConfig.getOssBucket(),
                                                           ossConfig.getOssRoleArn());
        } else {
          LOG.error("unknown external table storage {}",
                    task.tableMetaModel.odpsTableStorage);
          task.updateActionProgress(action, Progress.FAILED);
          throw new MmaException("Unknown external table storage " + task.tableMetaModel.odpsTableStorage + " when handle action " + action.name());
        }
      }

    switch (action) {
      case ODPS_DROP_TABLE:
        return OdpsSqlUtils.getDropTableStatement(task.tableMetaModel);
      case ODPS_CREATE_TABLE:
        return OdpsSqlUtils.getCreateTableStatement(task.tableMetaModel, externalTableConfig);
      case ODPS_DROP_PARTITION:
        return OdpsSqlUtils.getDropPartitionStatement(task.tableMetaModel);
      case ODPS_ADD_PARTITION:
        return OdpsSqlUtils.getAddPartitionStatement(task.tableMetaModel, externalTableConfig);
      case ODPS_LOAD_DATA:
        return OdpsSqlUtils.getInsertTableStatement(task.tableMetaModel);
      case ODPS_DESTINATION_VERIFICATION:
        return OdpsSqlUtils.getVerifySql(task.tableMetaModel);
      case ODPS_SOURCE_VERIFICATION:
        return OdpsSqlUtils.getVerifySql(task.tableMetaModel, false);
      default:
        throw new IllegalArgumentException("Unknown action " + action);
    }
  }

  @Override
  public void submitExecutionTask(Task task, Action action) throws MmaException {
    String sqlStatement = getSqlStatement(task, action);
    LOG.info("SQL Statement: {}", sqlStatement);
    if (sqlStatement.isEmpty()) {
      task.updateActionProgress(action, Progress.SUCCEEDED);
      LOG.error("Empty sqlStatement, mark done, action: {}", action);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    LOG.info("Submit {} task: {}, action: {}, taskProgress: {}",
        runnerType.name(), task, action.name(), task.progress);

    this.runnerPool.execute(new OdpsSqlExecutor(sqlStatement, task, action));
  }

  @Override
  public void shutdown() {
    super.shutdown();
  }
}
