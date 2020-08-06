package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager.MigrationStatus;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.RestoreTaskInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_FUNCTION_FOLDER;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_RESOURCE_FOLDER;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_VIEW_FOLDER;

public class OdpsDatabaseRestoreAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsDatabaseRestoreAction.class);

  private MmaConfig.ObjectType type;
  private MmaConfig.DatabaseRestoreConfig restoreConfig;
  private MmaMetaManager mmaMetaManager;
  private int limit = 100;

  private static List<MigrationStatus> ALL_STATUS = new ArrayList<>();
  private static List<MigrationStatus> ACTIVE_STATUS = new ArrayList<>();
  static {
    for(MigrationStatus status : MigrationStatus.values()) {
      ALL_STATUS.add(status);
    }
    ACTIVE_STATUS.add(MigrationStatus.PENDING);
    ACTIVE_STATUS.add(MigrationStatus.RUNNING);
  }

  public OdpsDatabaseRestoreAction(String id,
                                   MmaConfig.ObjectType type,
                                   MmaConfig.DatabaseRestoreConfig restoreConfig,
                                   MmaMetaManager mmaMetaManager) {
    super(id);
    this.type = type;
    this.restoreConfig = restoreConfig;
    this.mmaMetaManager = mmaMetaManager;
  }

  @Override
  public void doAction() throws MmaException {
    mergeJobInfoIntoRestoreDB();
    waitForTasksFinish();
  }

  private void mergeJobInfoIntoRestoreDB() throws MmaException {
    if (!restoreConfig.getRestoreTypes().contains(type)) {
      LOG.info("Action {} skip {} in database restore {}", id, type, GsonUtils.toJson(restoreConfig));
      return;
    }

    List<String> allOssObjects = getOssObjectList();
    LOG.info("MergeDatabaseRestoreMeta db {}, type {}, all oss objects {}",
        restoreConfig.getOriginDatabaseName(), type, GsonUtils.toJson(allOssObjects));

    List<RestoreTaskInfo> recoveredObjects = mmaMetaManager.listRestoreJobs(
        getWhereConditionStatement(ALL_STATUS), -1);

    // include SUCCEEDED and FAILED
    Set<String> finishedObjects = new HashSet<>();

    // key: object, value: attempts, include PENDING and RUNNING
    Map<String, Integer> activeObjects = new HashMap<>();

    for(RestoreTaskInfo taskInfo : recoveredObjects) {
      MigrationStatus status = taskInfo.getStatus();
      if (MigrationStatus.SUCCEEDED.equals(status) || MigrationStatus.FAILED.equals(status)) {
        finishedObjects.add(taskInfo.getObject());
      } else {
        activeObjects.put(taskInfo.getObject(), taskInfo.getAttemptTimes());
      }
    }

    List<RestoreTaskInfo> allTasks = new ArrayList<>();
    for (String objectName : allOssObjects) {
      if (finishedObjects.contains(objectName)) {
        continue;
      }
      int attemptTimes = activeObjects.containsKey(objectName) ?
          activeObjects.get(objectName) + 1 :
          Constants.MMA_OBJ_RESTORE_INIT_ATTEMPT_TIMES;
      MmaConfig.JobConfig jobConfig = new MmaConfig.JobConfig(
          restoreConfig.getOriginDatabaseName(),
          objectName,
          MmaConfig.JobType.RESTORE,
          getJobDescription(objectName, type),
          restoreConfig.getAdditionalTableConfig());
      allTasks.add(new RestoreTaskInfo(
          restoreConfig.getTaskName(),
          type.name(),
          restoreConfig.getOriginDatabaseName(),
          objectName,
          jobConfig,
          MigrationStatus.PENDING,
          attemptTimes,
          System.currentTimeMillis()));
    }
    for (RestoreTaskInfo taskInfo : allTasks) {
      mmaMetaManager.mergeJobInfoIntoRestoreDB(taskInfo);
    }
  }

  private void waitForTasksFinish() throws MmaException {
    String originDatabase = restoreConfig.getOriginDatabaseName();
    String destinationDatabase = restoreConfig.getDestinationDatabaseName();

    try {
      while (true) {
        List<RestoreTaskInfo> activeTasks = mmaMetaManager.listRestoreJobs(
            getWhereConditionStatement(ACTIVE_STATUS),
            limit);

        List<RestoreTaskInfo> failedTasks = mmaMetaManager.listRestoreJobs(
            getWhereConditionStatement(MigrationStatus.FAILED),
            limit);

        if (activeTasks.isEmpty()) {
          if (failedTasks.isEmpty()) {
            LOG.info("Action {} from {} to {} finished, type {}",
                id, originDatabase, destinationDatabase, type);
            return;
          }
          LOG.error("{} Wait database restore {} from {} to {} failed, failed tasks: {}",
              id,
              type,
              originDatabase,
              destinationDatabase,
              failedTasks.stream().map(k -> k.getObject() + ", attempts: " + k.getAttemptTimes() + "\n").collect(Collectors.toList()));
          throw new MmaException("Restore database failed " + GsonUtils.toJson(restoreConfig));
        }
        LOG.info("{} restore database {} from {} to {}, active tasks: {}",
            id,
            type,
            originDatabase,
            destinationDatabase,
            activeTasks.stream().map(k -> k.getObject() + "\n").collect(Collectors.toList()));
        Thread.sleep(10000);
      }
    } catch (Exception e) {
      throw new MmaException("Restore database failed " + GsonUtils.toJson(restoreConfig), e);
    }
  }

  private String getJobDescription(String object, MmaConfig.ObjectType type) {
    MmaConfig.ObjectRestoreConfig objectRestoreConfig = new MmaConfig.ObjectRestoreConfig(
        restoreConfig.getOriginDatabaseName(),
        restoreConfig.getDestinationDatabaseName(),
        object,
        type,
        restoreConfig.isUpdate(),
        restoreConfig.getTaskName(),
        restoreConfig.getAdditionalTableConfig(),
        restoreConfig.getSettings());
    return GsonUtils.toJson(objectRestoreConfig);
  }

  private String getWhereConditionStatement(MigrationStatus status) {
    StringBuilder builder = new StringBuilder("WHERE ");
    builder.append(String.format("%s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID, restoreConfig.getTaskName()));
    builder.append(String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_DB_NAME, restoreConfig.getOriginDatabaseName()));
    builder.append(String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_TYPE, type.toString()));
    builder.append(String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_STATUS, status));
    return builder.toString();
  }

  private String getWhereConditionStatement(List<MigrationStatus> status) {
    if (status.size() == 1) {
      return getWhereConditionStatement(status.get(0));
    }
    StringBuilder builder = new StringBuilder("WHERE ");
    builder.append(String.format("%s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_UNIQUE_ID, restoreConfig.getTaskName()));
    builder.append(String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_DB_NAME, restoreConfig.getOriginDatabaseName()));
    builder.append(String.format("AND %s='%s'\n", Constants.MMA_OBJ_RESTORE_COL_TYPE, type.toString()));
    builder.append(String.format("AND %s in (", Constants.MMA_OBJ_RESTORE_COL_STATUS));
    for (int index = 0; index < status.size(); index++) {
      builder.append(String.format("%s'%s'", index == 0 ? "" : ",", status.get(index)));
    }
    builder.append(")\n");
    return builder.toString();
  }

  private List<String> getOssObjectList() throws MmaException {
    String delimiter = "/";
    String folder;
    switch (type) {
      case TABLE:
        folder = EXPORT_TABLE_FOLDER;
        break;
      case VIEW:
        folder = EXPORT_VIEW_FOLDER;
        break;
      case RESOURCE:
        folder = EXPORT_RESOURCE_FOLDER;
        break;
      case FUNCTION:
        folder = EXPORT_FUNCTION_FOLDER;
        break;
      default:
        throw new MmaException("Unknown object type " + type + " in action " + id + ", restore config: " + GsonUtils.toJson(restoreConfig));
    }
    String prefix = OssUtils.getOssFolderToExportObject(
        restoreConfig.getTaskName(),
        folder,
        restoreConfig.getOriginDatabaseName());

    return OssUtils.listBucket(prefix, delimiter);
  }
}
