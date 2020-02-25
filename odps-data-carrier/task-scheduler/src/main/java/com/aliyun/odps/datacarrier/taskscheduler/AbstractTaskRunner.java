package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.utils.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class AbstractTaskRunner implements TaskRunner {

  private static final Logger LOG = LogManager.getLogger(AbstractTaskRunner.class);

  private static final int CORE_POOL_SIZE = 20;
  protected ThreadPoolExecutor runnerPool;

  public AbstractTaskRunner() {
    ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat(
        "TaskRunner-" + " #%d").setDaemon(true).build();
    this.runnerPool = new ThreadPoolExecutor(
        CORE_POOL_SIZE,
        Integer.MAX_VALUE,
        1,
        TimeUnit.HOURS,
        new LinkedBlockingQueue<>(), factory,
        new RunnerRejectedExecutionHandler());
  }

  private static class RunnerRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      System.out.print("Can't submit task to ThreadPoolExecutor:" + executor);
    }
  }


  private String generateSqlStatement(Task task, Action action) {
    StringBuilder sb = new StringBuilder();
    switch (action) {
      case ODPS_CREATE_TABLE:
        if (task.tableMetaModel.partitions.isEmpty()) {
          //Non-partition table should drop table at first.
          sb.append(OdpsSqlUtils.getDropTableStatement(task.tableMetaModel));
        }
        sb.append(OdpsSqlUtils.getCreateTableStatement(task.tableMetaModel));
        return sb.toString();
      case ODPS_ADD_PARTITION:
        sb.append(OdpsSqlUtils.getAddPartitionStatement(task.tableMetaModel));
        return sb.toString();
      case HIVE_LOAD_DATA:
        sb.append(HiveSqlUtils.getUdtfSql(task.tableMetaModel));
        return sb.toString();
      case HIVE_VALIDATE:
        sb.append(HiveSqlUtils.getVerifySql(task.tableMetaModel));
        return sb.toString();
      case ODPS_VALIDATE:
        sb.append(OdpsSqlUtils.getVerifySql(task.tableMetaModel));
        return sb.toString();
      default:
        return "";
    }
  }

  public void submitExecutionTask(Task task, Action action, String executionTaskName) {
    AbstractExecutionInfo executionInfo = task.actionInfoMap.get(action).executionInfoMap.get(executionTaskName);
    Path sqlPath = executionInfo.getSqlPath();
    String sqlStr = "";
    if (executionInfo.isScriptMode()) {
      try {
        sqlStr = new String(Files.readAllBytes(sqlPath));
      } catch (IOException e) {
        LOG.error("Submit execution task failed, " +
            "executionTaskName: " + executionTaskName +
            "path: " + sqlPath.toString());
        task.changeExecutionProgress(action, executionTaskName, Progress.FAILED);
        return;
      }
    } else if (StringUtils.isNullOrEmpty(executionInfo.getSqlStatements())) {
      sqlStr = executionInfo.getSqlStatements();
    } else {
      sqlStr = generateSqlStatement(task, action);
    }
    if (StringUtils.isNullOrEmpty(sqlStr)) {
      task.changeExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
      LOG.error("Empty sqlStatement, mark done, "  +
          "executionTaskName: " + executionTaskName);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    LOG.info("Submit {} task: {}, action: {}, executionTaskName: {}, taskProgress: {}",
        runnerType.name(), task, action.name(), executionTaskName, task.progress);
    if (RunnerType.HIVE.equals(runnerType)) {
      this.runnerPool.execute(new HiveRunner.HiveSqlExecutor(sqlStr, task, action, executionTaskName));
    } else if (RunnerType.ODPS.equals(runnerType)) {
      this.runnerPool.execute(new OdpsRunner.OdpsSqlExecutor(sqlStr, task, action, executionTaskName));
    }
  }

  @Override
  public void shutdown() {
    runnerPool.shutdown();
  }
}
