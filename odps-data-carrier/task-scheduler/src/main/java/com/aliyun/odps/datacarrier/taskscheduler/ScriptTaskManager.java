package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.aliyun.odps.datacarrier.commons.IntermediateDataManager;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import static com.aliyun.odps.datacarrier.commons.IntermediateDataManager.*;


class ScriptTaskManager implements TaskManager {

  private static final Logger LOG = LogManager.getLogger(ScriptTaskManager.class);

  protected Map<RunnerType, TaskRunner> taskRunnerMap;
  private String inputPath;
  private IntermediateDataManager intermediateDataManager;
  private String jdbcAddress;
  private String user;
  private String password;

  public ScriptTaskManager(String inputPath, SortedSet<Action> actions, Mode mode, String jdbcAddress, String user, String password) {
    this.taskRunnerMap = new ConcurrentHashMap<>();
    this.inputPath = inputPath;
    this.jdbcAddress = jdbcAddress;
    this.user = user;
    this.password = password;
    this.intermediateDataManager = new IntermediateDataManager(inputPath);
    generateTasks(actions, mode);
  }

  @Override
  public TaskRunner getTaskRunner(RunnerType runnerType) {
    return taskRunnerMap.get(runnerType);
  }

  /**
   * ├── catalog_sales
   * │   ├── hive_udtf_sql
   * │   │   └── multi_partition
   * │   │       └── catalog_sales.sql
   * │   ├── odps_ddl
   * │   │   └── create_table.sql
   * │   ├── odps_external_ddl
   * │   │   └── create_table.sql
   * │   └── odps_oss_transfer_sql
   * │       └── multi_partition
   * │           └── catalog_sales.sql
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
        Path tableDir = Paths.get(this.inputPath, dataBase, tableName);

        for (String sqlScriptDir : DirUtils.listDirs(tableDir)) {
          Path sqlScriptDirPath = Paths.get(tableDir.toString(), sqlScriptDir);

          Action action = CommonUtils.getSqlActionFromDir(sqlScriptDir);

          if (action.equals(Action.HIVE_VALIDATE) || action.equals(Action.ODPS_VALIDATE)) {
            LOG.info("Generate validate tasks by DataValidator, ignorint these files {}.", sqlScriptDirPath);
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
            if (!taskRunnerMap.containsKey(runnerType)) {
              taskRunnerMap.put(runnerType, createTaskRunner(runnerType));
              LOG.info("Find runnerType = {}, Add Runner: {}", runnerType, taskRunnerMap.get(runnerType).getClass());
            }

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

  private TaskRunner createTaskRunner(RunnerType runnerType) {
    if (RunnerType.HIVE.equals(runnerType)) {
      return new HiveRunner(this.jdbcAddress, this.user, this.password);
    } else if (RunnerType.ODPS.equals(runnerType)) {
      return new OdpsRunner();
    }
    throw new RuntimeException("Unknown runner type: " + runnerType.name());
  }

  public void shutdown() {
    for (TaskRunner runner : taskRunnerMap.values()) {
      runner.shutdown();
    }
  }
}
