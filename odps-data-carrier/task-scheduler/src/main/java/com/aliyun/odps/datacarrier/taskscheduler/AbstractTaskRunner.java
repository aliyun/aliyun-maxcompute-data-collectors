package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.utils.StringUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
      case HIVE_LOAD_DATA:
        sqlStatements.add(HiveSqlUtils.getUdtfSql(task.tableMetaModel));
        return sqlStatements;
      case HIVE_VALIDATE:
        sqlStatements.add(HiveSqlUtils.getVerifySql(task.tableMetaModel));
        return sqlStatements;
      case ODPS_VALIDATE:
        sqlStatements.add(OdpsSqlUtils.getVerifySql(task.tableMetaModel));
        return sqlStatements;
      default:
        return sqlStatements;
    }
  }

  public void submitExecutionTask(Task task, Action action, String executionTaskName) {
    AbstractExecutionInfo executionInfo =
        task.actionInfoMap.get(action).executionInfoMap.get(executionTaskName);

    List<String> sqlStatements = getSqlStatements(task, action);
    LOG.info("SQL Statements: {}", String.join(", ", sqlStatements));
    if (sqlStatements.isEmpty()) {
      task.updateExecutionProgress(action, executionTaskName, Progress.SUCCEEDED);
      LOG.error("Empty sqlStatement, mark done, executionTaskName: " + executionTaskName);
      return;
    }
    RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
    LOG.info("Submit {} task: {}, action: {}, executionTaskName: {}, taskProgress: {}",
        runnerType.name(), task, action.name(), executionTaskName, task.progress);
    // TODO: this is not clear, HiveRunner and OdpsRunner should impl their own submitExecutionTask
    //  method
    if (RunnerType.HIVE.equals(runnerType)) {
      this.runnerPool.execute(new HiveRunner.HiveSqlExecutor(sqlStatements,
                                                             task,
                                                             action,
                                                             executionTaskName));
    } else if (RunnerType.ODPS.equals(runnerType)) {
      this.runnerPool.execute(new OdpsRunner.OdpsSqlExecutor(sqlStatements,
                                                             task,
                                                             action,
                                                             executionTaskName));
    }
  }

  @Override
  public void shutdown() {
    runnerPool.shutdown();
  }
}
