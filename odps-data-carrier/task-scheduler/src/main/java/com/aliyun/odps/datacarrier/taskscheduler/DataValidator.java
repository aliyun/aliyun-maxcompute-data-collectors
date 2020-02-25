package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource.PartitionMetaModel;
import com.aliyun.odps.utils.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataValidator {

  private static final Logger LOG = LogManager.getLogger(DataValidator.class);

  private static String COUNT_VALIDATION_TASK = "count_validation_task";

  // hive.database -> odps.project
  private Map<String, String> projectMap = new HashMap<>();
  // hive.database -> (hive.database.tablename -> odps.database.tablename)
  private Map<String, Map<String, String>> tableMap = new HashMap<>();

  /**
   * Generate validate tasks according to task mapping.
   * @param tableMappingFilePath
   * @param tasks
   * @param where
   */
  public void generateValidateActions(String tableMappingFilePath, List<Task> tasks, String where) {
    getTableMapping(tableMappingFilePath);
    for (Task task : tasks) {
      String hiveDB = task.getProject();
      if (StringUtils.isNullOrEmpty(hiveDB)) {
        LOG.error("Wrong table mapping, task {}", task);
        continue;
      }
      task.addExecutionInfo(Action.HIVE_VALIDATE, COUNT_VALIDATION_TASK, new HiveExecutionInfo(
          createCountValidationSqlStatement(Action.HIVE_VALIDATE, hiveDB, task.getTableName(), where)));
      LOG.info("Add ExecutionInfo for {}, {}", Action.HIVE_VALIDATE, task.toString());
      String odpsProject = projectMap.get(hiveDB);
      if (StringUtils.isNullOrEmpty(odpsProject)) {
        LOG.error("Get ODPS project error, task {}", task);
        continue;
      }
      String odpsTable = "";
      if (tableMap.containsKey(hiveDB) && tableMap.get(hiveDB).containsKey(task.getTableName())) {
        odpsTable = tableMap.get(hiveDB).get(task.getTableName());
      }

      if (StringUtils.isNullOrEmpty(odpsTable)) {
        LOG.error("Get ODPS table error, task {}", task);
        continue;
      }

      task.addExecutionInfo(Action.ODPS_VALIDATE, COUNT_VALIDATION_TASK, new OdpsExecutionInfo(
          createCountValidationSqlStatement(Action.ODPS_VALIDATE, odpsProject, odpsTable, where)));
      LOG.info("Add ExecutionInfo for {}, {}", Action.ODPS_VALIDATE, task.toString());
      task.addActionInfo(Action.VALIDATION_BY_TABLE);
      LOG.info("Add ExecutionInfo for {}, {}", Action.VALIDATION_BY_TABLE, task.toString());
    }
  }

  private void getTableMapping(String tableMappingFilePath) {
    Path projectFilePath = Paths.get(System.getProperty("user.dir"), tableMappingFilePath);
    LOG.info("Start parsing table mapping for {}.", projectFilePath.toString());
    try {
      List<String> tableMappings = Files.readAllLines(projectFilePath);
      for (String tableMappingStr : tableMappings) {
        String[] mappings = tableMappingStr.split(":");
        if (mappings.length != 2) {
          LOG.error("Load table mappings failed, {}", tableMappingStr);
          continue;
        }
        String hiveDB = mappings[0].split("[.]")[0];
        String hiveTable = mappings[0].split("[.]")[1];
        String odpsProject = mappings[1].split("[.]")[0];
        String odpsTable = mappings[1].split("[.]")[1];
        projectMap.putIfAbsent(hiveDB, odpsProject);
        tableMap.putIfAbsent(hiveDB, new HashMap<>());
        Map<String, String> tableMapOfHiveDB = tableMap.get(hiveDB);
        tableMapOfHiveDB.putIfAbsent(hiveTable, odpsTable);
      }
    } catch (Exception e) {
      LOG.error("Load table mappings failed, {}", e.getMessage());
      e.printStackTrace();
    }
  }

  private String createCountValidationSqlStatement(Action action, String project, String table, String where) {
    StringBuilder sb = new StringBuilder("SELECT count(1) FROM ");
    sb.append(project).append(".`").append(table).append("`");
    sb.append("\nWHERE " + where + "\n");
    if (Action.ODPS_VALIDATE.equals(action)) {
      sb.append(";");
    }
    return sb.toString();
  }

  public boolean validateAllTasksCountResult(List<Task> tasks) {
    boolean result = true;
    for (Task task : tasks) {
      if (task.actionInfoMap.containsKey(Action.HIVE_VALIDATE) &&
          task.actionInfoMap.containsKey(Action.ODPS_VALIDATE)) {
        String hiveResult =
            task.actionInfoMap.get(Action.HIVE_VALIDATE).executionInfoMap.get(COUNT_VALIDATION_TASK).getResult();
        String odpsResult =
            task.actionInfoMap.get(Action.ODPS_VALIDATE).executionInfoMap.get(COUNT_VALIDATION_TASK).getResult();
        if (StringUtils.isNullOrEmpty(hiveResult) || StringUtils.isNullOrEmpty(odpsResult)) {
          LOG.info("{} Validate FAIL. --> HiveResult: {}, OdpsResult: {}.", task, hiveResult, odpsResult);
          result = false;
          continue;
        }
        try {
          long hiveCount = Long.valueOf(hiveResult);
          long odpsCount = Long.valueOf(odpsResult);

          if (hiveCount == odpsCount) {
            LOG.info("{} Validate PASS. --> HiveCount: {}, OdpsCount: {}.", task, hiveCount, odpsCount);
          } else {
            LOG.info("{} Validate FAIL. --> HiveResult: {}, OdpsResult: {}.", task, hiveResult, odpsResult);
            result = false;
          }
        } catch (Exception e) {
          LOG.info("{} Parse result fail. --> HiveResult: {}, OdpsResult: {}", task, hiveResult, odpsResult);
          result = false;
          continue;
        }

      }
    }
    return result;
  }

  public boolean validateTaskCountResult(Task task) {
    if (task.actionInfoMap.containsKey(Action.HIVE_VALIDATE) &&
        task.actionInfoMap.containsKey(Action.ODPS_VALIDATE)) {
      String hiveResult =
          task.actionInfoMap.get(Action.HIVE_VALIDATE).executionInfoMap.get(COUNT_VALIDATION_TASK).getResult();
      String odpsResult =
          task.actionInfoMap.get(Action.ODPS_VALIDATE).executionInfoMap.get(COUNT_VALIDATION_TASK).getResult();
      if (StringUtils.isNullOrEmpty(hiveResult) || StringUtils.isNullOrEmpty(odpsResult)) {
        LOG.info("{} Validate FAIL. --> HiveResult: {}, OdpsResult: {}.", task, hiveResult, odpsResult);
        return false;
      }
      try {
        long hiveCount = Long.valueOf(hiveResult);
        long odpsCount = Long.valueOf(odpsResult);

        if (hiveCount == odpsCount) {
          LOG.info("{} Validate PASS. --> HiveCount: {}, OdpsCount: {}.", task, hiveCount, odpsCount);
          return true;
        } else {
          LOG.info("{} Validate FAIL. --> HiveResult: {}, OdpsResult: {}.", task, hiveResult, odpsResult);
          return false;
        }
      } catch (Exception e) {
        LOG.info("{} Parse result fail. --> HiveResult: {}, OdpsResult: {}", task, hiveResult, odpsResult);
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * hive result:
   * 393394
   * 12273095
   * 11852976
   * 393394
   * 12273095
   * 11852976
   * 393394
   *
   * odps result:
   * _c0
   * 393394
   * 12273095
   * 11852976
   * 393394
   * 12273095
   * 11852976
   * 393394
   * @param task
   * @return
   */
  public ValidationResult validationPartitions(Task task) {
    if (task.actionInfoMap.containsKey(Action.HIVE_VALIDATE) &&
        task.actionInfoMap.containsKey(Action.ODPS_VALIDATE)) {
      Map<String, String> hiveResults =
          task.actionInfoMap.get(Action.HIVE_VALIDATE).executionInfoMap.get(task.getTableNameWithProject()).getMultiRecordResult();
      Map<String, String> odpsResults =
          task.actionInfoMap.get(Action.ODPS_VALIDATE).executionInfoMap.get(task.getTableNameWithProject()).getMultiRecordResult();
      if (hiveResults.size() != odpsResults.size()) {
        LOG.warn("{} Validate ERROR! --> HivePartitionCount: {}, OdpsPartitionCount: {}.",
            task, hiveResults.size(), odpsResults.size());
        return null;
      }
      ValidationResult validationResult = new ValidationResult();
      for (PartitionMetaModel partitionMetaModel : task.tableMetaModel.partitions) {
        //TODO[mingyou] need to support multiple partition key.
        String partitionValue = partitionMetaModel.partitionValues.get(0);
        if (!hiveResults.containsKey(partitionValue) || !odpsResults.containsKey(partitionValue)
        || !hiveResults.get(partitionValue).equals(odpsResults.get(partitionValue))) {
          validationResult.failedPartitions.add(partitionMetaModel.partitionValues);
        } else {
          validationResult.succeededPartitions.add(partitionMetaModel.partitionValues);
        }
      }
      return validationResult;
    }
    return null;
  }

  public class ValidationResult {
    List<List<String>> succeededPartitions = new ArrayList<>();
    List<List<String>> failedPartitions = new ArrayList<>();
  }
}
