package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.aliyun.odps.datacarrier.commons.IntermediateDataManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import static com.aliyun.odps.datacarrier.commons.IntermediateDataManager.*;


class ScriptTaskManager implements TaskManager {

  private static final Logger LOG = LogManager.getLogger(ScriptTaskManager.class);

  private Set<String> finishedTasks;
  private String inputPath;
  private IntermediateDataManager intermediateDataManager;

  public ScriptTaskManager(Set<String> finishedTasks, String inputPath, SortedSet<Action> actions, Mode mode) {
    this.finishedTasks = finishedTasks;
    this.inputPath = inputPath;
    this.intermediateDataManager = new IntermediateDataManager(inputPath);
    generateTasks(actions, mode);
  }

  /**
   * inventory/
   * ├── hive_udtf_sql
   * │   └── multi_partition
   * │       ├── inventory_0.sql
   * │       ├── inventory_1.sql
   * │       └── inventory_2.sql
   * ├── hive_verify_sql
   * │   ├── inventory_0.sql
   * │   ├── inventory_1.sql
   * │   └── inventory_2.sql
   * ├── odps_ddl
   * │   ├── create_partition_0.sql
   * │   ├── create_partition_1.sql
   * │   ├── create_partition_2.sql
   * │   └── create_table.sql
   * ├── odps_external_ddl
   * │   ├── create_partition_0.sql
   * │   └── create_table.sql
   * ├── odps_oss_transfer_sql
   * │   ├── multi_partition
   * │   │   └── inventory.sql
   * │   └── single_partition
   * │       └── multi_insert_0.sql
   * └── odps_verify_sql
   * ├── inventory_0.sql
   * ├── inventory_1.sql
   * └── inventory_2.sql
   *
   * @param actions
   * @param mode
   */
  @Override
  public List<Task> generateTasks(SortedSet<Action> actions, Mode mode) {
    List<Task> tasks = new LinkedList<>();
    for (String dataBase : intermediateDataManager.listDatabases()) {
      for (String tableName : intermediateDataManager.listTables(dataBase)) {
        LOG.info("Start generating tasks for [{}.{}]", dataBase, tableName);
        Task task = new Task(dataBase, tableName);

        if (finishedTasks.contains(task.getTableNameWithProject())) {
          LOG.info("Task {} already finished, skip generate task", task.toString());
          continue;
        }

        Path tableDir = Paths.get(this.inputPath, dataBase, tableName);

        for (String sqlScriptDir : DirUtils.listDirs(tableDir)) {
          Path sqlScriptDirPath = Paths.get(tableDir.toString(), sqlScriptDir);

          Action action = CommonUtils.getSqlActionFromDir(sqlScriptDir);

          if (action.equals(Action.HIVE_VALIDATE) || action.equals(Action.ODPS_VALIDATE)) {
            LOG.info("Generate validate tasks by DataValidator, ignore these files {}.", sqlScriptDirPath);
            continue;
          }

          if (Action.HIVE_LOAD_DATA.equals(action) || Action.ODPS_LOAD_DATA.equals(action)) {
            if (Mode.BATCH.equals(mode)) {
              sqlScriptDirPath = Paths.get(sqlScriptDirPath.toString(), MULTI_PARTITION_DIR);
            } else if (Mode.SINGLE.equals(mode)) {
              sqlScriptDirPath = Paths.get(sqlScriptDirPath.toString(), SINGLE_PARTITION_DIR);
            }
          }

          for (String sqlScript : DirUtils.listFiles(sqlScriptDirPath)) {
            action = CommonUtils.getSqlActionFromDir(sqlScriptDir);
            if (Action.ODPS_DDL.equals(action)) {
              if (sqlScript.indexOf(CREATE_TABLE_FILENAME) != -1) {
                action = Action.ODPS_CREATE_TABLE;
              } else if (sqlScript.indexOf(CREATE_PARTITION_PREFIX) != -1) {
                action = Action.ODPS_ADD_PARTITION;
              }
            } else if (Action.ODPS_EXTERNAL_DDL.equals(action)) {
              if (sqlScript.indexOf(CREATE_TABLE_FILENAME) != -1) {
                action = Action.ODPS_CREATE_EXTERNAL_TABLE;
              } else if (sqlScript.indexOf(CREATE_PARTITION_PREFIX) != -1) {
                action = Action.ODPS_ADD_EXTERNAL_TABLE_PARTITION;
              }
            }

            Path sqlPath = Paths.get(sqlScriptDirPath.toString(), sqlScript);
            LOG.info("[{}.{}]: SQL={}, Action={}", dataBase, tableName, sqlPath, action.name());

            if (Action.UNKNOWN.equals(action)) {
              LOG.warn("Can not transfer " + sqlPath.toString() + " to Odps Task.");
              continue;
            }
            if (!actions.contains(action)) {
              LOG.warn("No needed action {}", action);
              continue;
            }

            //Create task runner.
            RunnerType runnerType = CommonUtils.getRunnerTypeByAction(action);
            if (RunnerType.ODPS.equals(runnerType)) {
              task.addExecutionInfo(action, sqlScript, new OdpsExecutionInfo(sqlPath));
            } else if (RunnerType.HIVE.equals(runnerType)) {
              task.addExecutionInfo(action, sqlScript, new HiveExecutionInfo(sqlPath));
            }
          }
        }
        tasks.add(task);
      }
    }
    return tasks;
  }
}
