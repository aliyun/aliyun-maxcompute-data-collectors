package com.aliyun.odps.datacarrier.taskscheduler;


import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataValidator {

  private static final Logger LOG = LoggerFactory.getLogger(DataValidator.class);

  private static String COUNT_VALIDATION_TASK = "count_validation_task";
  private static String TABLE_MAPPING_PATH = "table_mappings";

  // hive.database -> odps.project
  private Map<String, String> projectMap = new HashMap<>();
  // hive.database -> (hive.database.tablename -> odps.database.tablename)
  private Map<String, Map<String, String>> tableMap = new HashMap<>();

  public void generateValidateActions(List<Task> tasks) {
    getTableMapping();
    for (Task task : tasks) {
      String hiveDB = task.getProject();
      if (StringUtils.isNullOrEmpty(hiveDB)) {
        LOG.error("Wrong table mapping, task {}", task);
        continue;
      }
      task.addExecutionInfo(Action.HIVE_VALIDATE, COUNT_VALIDATION_TASK, new HiveExecutionInfo(
          createCountValidationSqlStatement(Action.HIVE_VALIDATE, hiveDB, task.getTableName())));

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
          createCountValidationSqlStatement(Action.ODPS_VALIDATE, odpsProject, odpsTable)));
      task.addActionInfo(Action.VALIDATION);
    }
  }

  private void getTableMapping() {
    Path tableMappingPath = Paths.get(System.getProperty("user.dir"), TABLE_MAPPING_PATH);
    for (String projectFileName : DirUtils.listFiles(tableMappingPath)) {
      Path projectFilePath = Paths.get(tableMappingPath.toString(), projectFileName);
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
  }

  private String createCountValidationSqlStatement(Action action, String project, String table) {
    StringBuilder sb = new StringBuilder("SELECT count(1) FROM ");
    sb.append(project).append(".`").append(table).append("`");
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
}
