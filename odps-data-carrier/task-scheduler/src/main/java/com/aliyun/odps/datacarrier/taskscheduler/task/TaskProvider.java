package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.datacarrier.taskscheduler.BackgroundLoopManager;
import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.DropRestoredTemporaryTableWorkItem;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.ObjectType;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.action.AddBackgroundWorkItemAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.AddMigrationJobAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDatabaseRestoreDeleteMetaAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportTableDDLAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportViewAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsResetTableMetaModelAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestorePartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDatabaseRestoreAction;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.RestoreTaskInfo;
import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableStorage;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsCreateOssExternalTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropPartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveSourceVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveUdtfDataTransferAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsAddPartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsCreateTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDestVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsInsertOverwriteDataTransferAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsSourceVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.VerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.utils.StringUtils;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_PARTITION_SPEC_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_TABLE_FOLDER;

public class TaskProvider {
  private static final Logger LOG = LogManager.getLogger(TaskProvider.class);

  private MmaMetaManager mmaMetaManager;
  private BackgroundLoopManager backgroundLoopManager;

  private List<Task> pendingTasks = new ArrayList<>();

  public TaskProvider(MmaMetaManager mmaMetaManager) {
    this.mmaMetaManager = Objects.requireNonNull(mmaMetaManager);
    backgroundLoopManager = new BackgroundLoopManager();
    backgroundLoopManager.start();
  }

  public synchronized List<Task> get() throws MmaException {
    List<Task> ret = new LinkedList<>();
    ret.addAll(getTasksFromMetaDB());
    ret.addAll(getTasksFromRestoreDB());
    synchronized (pendingTasks) {
      ret.addAll(pendingTasks);
      pendingTasks.clear();
    }
    return ret;
  }

  public synchronized List<Task> getTasksFromMetaDB() throws MmaException {
    List<TableMetaModel> pendingTables = mmaMetaManager.getPendingTables();
    if (!pendingTables.isEmpty()) {
      LOG.info("Tables to migrate");
      for (TableMetaModel tableMetaModel : pendingTables) {
        LOG.info("Database: {}, table: {}",
                 tableMetaModel.databaseName,
                 tableMetaModel.tableName);
      }
    } else {
      LOG.info("No pending table or partition found");
    }

    setRunning(pendingTables);

    List<Task> ret = new LinkedList<>();
    DataSource datasource = MmaServerConfig.getInstance().getDataSource();
    for (TableMetaModel tableMetaModel : pendingTables) {
      MmaConfig.JobConfig config =
          mmaMetaManager.getConfig(tableMetaModel.databaseName, tableMetaModel.tableName);

      MmaConfig.JobType jobType = config.getJobType();
      if (MmaConfig.JobType.MIGRATION.equals(jobType)) {
        TableMigrationConfig tableMigrationConfig = TableMigrationConfig.fromJson(config.getDescription());
        if (tableMetaModel.partitionColumns.isEmpty()) {
          ret.add(generateNonPartitionedTableMigrationTask(datasource, tableMetaModel, tableMigrationConfig));
        } else {
          ret.addAll(generatePartitionedTableMigrationTask(datasource, tableMetaModel, tableMigrationConfig));
        }
      } else if (MmaConfig.JobType.BACKUP.equals(jobType)) {
        MmaConfig.ObjectExportConfig exportConfig = MmaConfig.ObjectExportConfig.fromJson(config.getDescription());
        String taskName = exportConfig.getTaskName();
        Task task = null;
        switch (exportConfig.getObjectType()) {
          case TABLE: {
            ret.addAll(generateTableExportTasks(taskName,
                tableMetaModel,
                config.getAdditionalTableConfig()));
            break;
          }
          case VIEW: {
            task = generateViewExportTask(taskName, tableMetaModel);
            break;
          }
          case FUNCTION: {
            task = generateFunctionExportTask(taskName, tableMetaModel);
            break;
          }
          case RESOURCE: {
            task = generateResourceExportTask(taskName, tableMetaModel);
            break;
          }
          default:
            LOG.error("Unsupported meta type {} when backup {}.{} task {} to OSS",
                exportConfig.getObjectType(), exportConfig.getDatabaseName(), exportConfig.getObjectName(), exportConfig.getTaskName());
        }
        if (task != null) {
          ret.add(task);
        }
      } else if (MmaConfig.JobType.RESTORE.equals(jobType)) {
        Task task;
        if (Strings.isNullOrEmpty(tableMetaModel.tableName)) {
          MmaConfig.DatabaseRestoreConfig restoreConfig = MmaConfig.DatabaseRestoreConfig.fromJson(config.getDescription());
          task = generateDatabaseRestoreTask(restoreConfig, tableMetaModel);
        } else {
          MmaConfig.ObjectRestoreConfig restoreConfig = MmaConfig.ObjectRestoreConfig.fromJson(config.getDescription());
          task = generateObjectRestoreTask(restoreConfig, tableMetaModel, null);
        }
        if (task != null) {
          ret.add(task);
        }
      } else {
        LOG.error("Unsupported job type {} for {}.{}", jobType, tableMetaModel.databaseName, tableMetaModel.tableName);
        // TODO: should mark corresponding job as failed
      }
    }
    return ret;
  }

  public synchronized List<Task> getTasksFromRestoreDB() throws MmaException {
    List<Task> ret = new LinkedList<>();
    String condition = "WHERE status='PENDING'\n";
    List<RestoreTaskInfo> pendingTasks = mmaMetaManager.listRestoreJobs(condition, 100);
    if (pendingTasks.isEmpty()) {
      LOG.info("No pending restore tasks found.");
      return ret;
    }
    for (RestoreTaskInfo taskInfo : pendingTasks) {
      MmaConfig.ObjectRestoreConfig config = MmaConfig.ObjectRestoreConfig
          .fromJson(taskInfo.getJobConfig().getDescription());
      TableMetaModel tableMetaModel = new MetaSource.TableMetaModel();
      tableMetaModel.databaseName = config.getOriginDatabaseName();
      tableMetaModel.odpsProjectName = config.getDestinationDatabaseName();
      tableMetaModel.tableName = config.getObjectName();
      tableMetaModel.odpsTableName = config.getObjectName();
      Task task = generateObjectRestoreTask(config, tableMetaModel, taskInfo);
      if (task != null) {
        ((ObjectExportAndRestoreTask) task).setRestoreTaskInfo(taskInfo);
        taskInfo.setStatus(MmaMetaManager.MigrationStatus.RUNNING);
        taskInfo.setLastModifiedTime(System.currentTimeMillis());
        mmaMetaManager.mergeJobInfoIntoRestoreDB(taskInfo);
        ret.add(task);
      }
    }
    return ret;
  }

  public void addPendingTask(Task task) {
    synchronized (pendingTasks) {
      pendingTasks.add(task);
    }
  }

  private void setRunning(List<TableMetaModel> pendingTables) throws MmaException {
    for (TableMetaModel tableMetaModel : pendingTables) {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                  tableMetaModel.tableName,
                                  MmaMetaManager.MigrationStatus.RUNNING);
      if (tableMetaModel.partitionColumns.size() > 0 && !tableMetaModel.partitions.isEmpty()) {
        mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                    tableMetaModel.tableName,
                                    tableMetaModel.partitions
                                        .stream()
                                        .map(p -> p.partitionValues)
                                        .collect(Collectors.toList()),
                                    MmaMetaManager.MigrationStatus.RUNNING);
      }
    }
  }

  private Task generateNonPartitionedTableMigrationTask(
      DataSource datasource,
      TableMetaModel tableMetaModel,
      TableMigrationConfig config) {

    String taskId = getUniqueMigrationTaskName(
        tableMetaModel.databaseName,
        tableMetaModel.tableName);

    DirectedAcyclicGraph<Action, DefaultEdge> dag;
    switch (datasource) {
      case Hive:
        dag = getHiveNonPartitionedTableMigrationActionDag(taskId);
        break;
      case ODPS: {
        String destTableStorage = config.getDestTableStorage();
        if (!StringUtils.isNullOrEmpty(destTableStorage)) {
          ExternalTableStorage storage = ExternalTableStorage.valueOf(destTableStorage);
          String location = null;
          if (ExternalTableStorage.OSS.equals(storage)) {
            MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
            String ossFolder = tableMetaModel.odpsProjectName + ".db/" + tableMetaModel.odpsTableName + "/";
            location = OdpsSqlUtils.getOssTablePath(ossConfig.getOssEndpoint(), ossConfig.getOssBucket(), ossFolder);
          }
          ExternalTableConfig externalTableConfig = new ExternalTableConfig(storage, location);
          dag = getOdpsNonPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
        } else {
          dag = getOdpsNonPartitionedTableMigrationActionDag(taskId);
        }
        break;
      }
      case OSS:
      default:
        throw new UnsupportedOperationException();
    }

    return new MigrationTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private List<Task> generatePartitionedTableMigrationTask(
      DataSource datasource,
      TableMetaModel tableMetaModel,
      TableMigrationConfig config) {

    List<Task> ret = new LinkedList<>();

    if (tableMetaModel.partitions.isEmpty()) {
      String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName,
                                                 tableMetaModel.tableName);
      OdpsCreateTableAction createTableAction =
          new OdpsCreateTableAction(taskId + ".CreateTable");
      DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      dag.addVertex(createTableAction);
      Task task = new MigrationTask(taskId, tableMetaModel, dag, mmaMetaManager);
      ret.add(task);
      return ret;
    }

    String taskNamePrefix = getUniqueMigrationTaskName(
        tableMetaModel.databaseName,
        tableMetaModel.tableName);
    List<TableMetaModel> tableSplits = getTableSplits(tableMetaModel, config.getAdditionalTableConfig());

    for (int i = 0; i < tableSplits.size(); i++) {
      TableMetaModel split = tableSplits.get(i);
      String taskId = taskNamePrefix + "." + i;

      DirectedAcyclicGraph<Action, DefaultEdge> dag;
      switch (datasource) {
        case Hive:
          dag = getHivePartitionedTableMigrationActionDag(taskId);
          break;
        case ODPS: {
          String destTableStorage = config.getDestTableStorage();
          if (!StringUtils.isNullOrEmpty(destTableStorage)) {
            ExternalTableStorage storage = ExternalTableStorage.valueOf(destTableStorage);
            String location = null;
            if (ExternalTableStorage.OSS.equals(storage)) {
              MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
              String ossFolder = tableMetaModel.odpsProjectName + ".db/" + tableMetaModel.odpsTableName + "/";
              location = OdpsSqlUtils.getOssTablePath(ossConfig.getOssEndpoint(), ossConfig.getOssBucket(), ossFolder);
            }
            ExternalTableConfig externalTableConfig = new ExternalTableConfig(storage, location);
            dag = getOdpsPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
          } else {
            dag = getOdpsPartitionedTableMigrationActionDag(taskId);
          }
          break;
        }
        case OSS:
        default:
          throw new UnsupportedOperationException();
      }

      ret.add(new MigrationTask(taskId, split, dag, mmaMetaManager));
    }

    return ret;
  }

  private List<Task> generateTableExportTasks(String taskName,
                                              TableMetaModel tableMetaModel,
                                              MmaConfig.AdditionalTableConfig config) {
    String id = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    List<TableMetaModel> splitResult = getTableSplits(tableMetaModel, config);
    String location = OssUtils.getOssPathToExportObject(
        taskName,
        Constants.EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        Constants.EXPORT_TABLE_DATA_FOLDER);
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    ExternalTableConfig externalTableConfig = new ExternalTableConfig(ExternalTableStorage.OSS,
        OdpsSqlUtils.getOssTablePath(ossConfig.getOssEndpoint(), ossConfig.getOssBucket(), location));
    List<Task> tasks = new ArrayList<>();
    AtomicInteger lineageTasksCounter = new AtomicInteger(0);
    for (int index = 0; index < splitResult.size(); index++) {
      TableMetaModel model = splitResult.get(index);
      String taskId = id + (splitResult.size() == 1 ? "" : (".part#" + index));
      DirectedAcyclicGraph<Action, DefaultEdge> dag = null;
      if (tableMetaModel.partitionColumns.isEmpty()) {
        dag = getOdpsNonPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
      } else {
        dag = getOdpsPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
      }
      Action leafAction = null;
      for (Action action : dag.vertexSet()) {
        if (dag.getDescendants(action).isEmpty()) {
          leafAction = action;
          break;
        }
      }
      OdpsExportTableDDLAction exportTableDDLAction = new OdpsExportTableDDLAction(
          taskId + ".ExportTableDDL",
          taskName,
          tableMetaModel,
          lineageTasksCounter);
      dag.addVertex(exportTableDDLAction);
      dag.addEdge(leafAction, exportTableDDLAction);
      tasks.add(new MigrationTask(taskId, model, dag, mmaMetaManager));
    }
    return tasks;
  }

  private Task generateViewExportTask(String taskName, TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    Table table = OdpsUtils.getTable(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (table == null) {
      LOG.info("Table {}.{} not found", tableMetaModel.databaseName, tableMetaModel.tableName);
      return null;
    }
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsExportViewAction action = new OdpsExportViewAction(taskId + ".ExportViewDDL", taskName, table.getViewText());
    dag.addVertex(action);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateFunctionExportTask(String taskName, TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Function function = OdpsUtils.getFunction(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (function == null) {
      return null;
    }
    OdpsExportFunctionAction action = new OdpsExportFunctionAction(taskId + ".ExportFunctionDDL", taskName, function);
    dag.addVertex(action);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateResourceExportTask(String taskName, TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Resource resource = OdpsUtils.getResource(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (resource == null) {
      return null;
    }
    OdpsExportResourceAction action = new OdpsExportResourceAction(taskId + ".ExportResourceDDL", taskName, resource);
    dag.addVertex(action);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateObjectRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                         TableMetaModel tableMetaModel,
                                         RestoreTaskInfo taskInfo) {
    Task task = null;
    switch (restoreConfig.getObjectType()) {
      case FUNCTION:
        task = generateFunctionRestoreTask(restoreConfig, tableMetaModel);
        break;
      case RESOURCE:
        task = generateResourceRestoreTask(restoreConfig, tableMetaModel);
        break;
      case TABLE:
        task = generateTableRestore(restoreConfig, tableMetaModel, taskInfo);
        break;
      case VIEW:
        task = generateViewRestoreTask(restoreConfig, tableMetaModel);
        break;
      default:
        LOG.error("Unsupported restore type {}", MmaConfig.ObjectRestoreConfig.toJson(restoreConfig));
    }
    return task;
  }

  private Task generateTableRestore(MmaConfig.ObjectRestoreConfig restoreConfig,
                                    TableMetaModel tableMetaModel,
                                    RestoreTaskInfo restoreTaskInfo) {
    String ossFileName = OssUtils.getOssPathToExportObject(
        restoreConfig.getTaskName(),
        EXPORT_TABLE_FOLDER,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        EXPORT_PARTITION_SPEC_FILE_NAME);
    if (OssUtils.exists(ossFileName)) {
      return generatePartitionedTableRestoreTask(restoreConfig, tableMetaModel, restoreTaskInfo);
    } else {
      LOG.info("Restore partition info file not found for {}", restoreConfig);
      return generateNonPartitionedTableRestoreTask(restoreConfig, tableMetaModel);
    }
  }

  private Task generatePartitionedTableRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                                   TableMetaModel tableMetaModel,
                                                   RestoreTaskInfo restoreTaskInfo) {
    Task task = null;
    String temporaryTableName = generateRestoredTemporaryTableName(restoreConfig);
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    try {
      DropRestoredTemporaryTableWorkItem item = new DropRestoredTemporaryTableWorkItem(
          taskId + ".DropRestoredTemporaryTable",
          restoreConfig.getDestinationDatabaseName(),
          temporaryTableName,
          restoreConfig.getTaskName(),
          tableMetaModel,
          restoreTaskInfo,
          mmaMetaManager,
          this);
      if (!mmaMetaManager.hasMigrationJob(restoreConfig.getDestinationDatabaseName(), temporaryTableName)) {
        DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
        String taskName = restoreConfig.getTaskName();
        OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
            taskId + ".RestoreTable",
            taskName,
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            tableMetaModel.odpsProjectName,
            temporaryTableName,
            restoreConfig.getObjectType(),
            restoreConfig.getSettings());
        dag.addVertex(restoreTableAction);
        if (restoreConfig.isUpdate()) {
          OdpsDropTableAction dropTableAction = new OdpsDropTableAction(
              taskId + ".DropTableToBeRestored",
              false);
          dag.addVertex(dropTableAction);
          dag.addEdge(dropTableAction, restoreTableAction);
        }

        OdpsRestorePartitionAction restorePartitionAction = new OdpsRestorePartitionAction(
            taskId + ".RestorePartition",
            taskName,
            tableMetaModel.databaseName,
            tableMetaModel.tableName,
            tableMetaModel.odpsProjectName,
            temporaryTableName,
            restoreConfig.getSettings());

        TableMigrationConfig config = new TableMigrationConfig(
            tableMetaModel.odpsProjectName,
            temporaryTableName,
            tableMetaModel.odpsProjectName,
            tableMetaModel.tableName,
            restoreConfig.getAdditionalTableConfig());

        AddMigrationJobAction addMigrationJobAction = new AddMigrationJobAction(
            taskId + ".AddMigrationJobAction",
            config,
            mmaMetaManager);

        AddBackgroundWorkItemAction addBackgroundWorkItemAction = new AddBackgroundWorkItemAction(
            taskId + ".AddBackgroundWorkItem",
            backgroundLoopManager,
            item);

        dag.addVertex(restorePartitionAction);
        dag.addVertex(addMigrationJobAction);
        dag.addVertex(addBackgroundWorkItemAction);

        dag.addEdge(restoreTableAction, restorePartitionAction);
        dag.addEdge(restorePartitionAction, addMigrationJobAction);
        dag.addEdge(addMigrationJobAction, addBackgroundWorkItemAction);

        task = new OdpsRestoreTablePrepareTask(taskId, tableMetaModel, dag, mmaMetaManager);
      } else {
        LOG.info("Migration temporary table job already exist for {}", MmaConfig.ObjectRestoreConfig.toJson(restoreConfig));
        backgroundLoopManager.addWorkItem(item);
      }
    } catch (Exception e) {
      LOG.error("Exception when generate partitioned table restore task {}",
          MmaConfig.ObjectRestoreConfig.toJson(restoreConfig), e);
    }
    return task;
  }

  private String generateRestoredTemporaryTableName(MmaConfig.ObjectRestoreConfig restoreConfig) {
    return "temporary_table_to_restore_" + restoreConfig.getObjectName() + "_in_task_" + restoreConfig.getTaskName();
  }

  private Task generateNonPartitionedTableRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                                      TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    String taskName = restoreConfig.getTaskName();
    ObjectType type = restoreConfig.getObjectType();
    String temporaryTableName = tableMetaModel.tableName + "_restore_task_" + taskName + "_" + System.currentTimeMillis();
    OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
        taskId + ".Restore" + type.name(),
        taskName,
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        type,
        restoreConfig.getSettings());
    dag.addVertex(restoreTableAction);
    if (restoreConfig.isUpdate()) {
      OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".Drop" + type, ObjectType.VIEW.equals(type));
      dag.addVertex(dropTableAction);
      dag.addEdge(dropTableAction, restoreTableAction);
    }
    OdpsResetTableMetaModelAction resetTableMetaModelAction = new OdpsResetTableMetaModelAction(
        taskId + ".ResetTableMetaModel",
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        restoreConfig.getDestinationDatabaseName(),
        restoreConfig.getObjectName());
    dag.addVertex(resetTableMetaModelAction);
    dag.addEdge(restoreTableAction, resetTableMetaModelAction);
    OdpsCreateTableAction createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");
    OdpsDropTableAction dropTemporaryTableAction = new OdpsDropTableAction(
        taskId + ".DropTemporaryTable",
        tableMetaModel.odpsProjectName,
        temporaryTableName,
        false);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);
    dag.addVertex(dropTemporaryTableAction);

    dag.addEdge(resetTableMetaModelAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);
    dag.addEdge(verificationAction, dropTemporaryTableAction);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateViewRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                       TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreTableAction restoreTableAction = new OdpsRestoreTableAction(
        taskId + ".RestoreView",
        restoreConfig.getTaskName(),
        tableMetaModel.databaseName,
        tableMetaModel.tableName,
        tableMetaModel.odpsProjectName,
        tableMetaModel.tableName,
        restoreConfig.getObjectType(),
        restoreConfig.getSettings());
    dag.addVertex(restoreTableAction);
    if (restoreConfig.isUpdate()) {
      OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropView", true);
      dag.addVertex(dropTableAction);
      dag.addEdge(dropTableAction, restoreTableAction);
    }
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateDatabaseRestoreTask(MmaConfig.DatabaseRestoreConfig restoreConfig,
                                           TableMetaModel tableMetaModel) {
    String taskId = "RestoreDatabase." + restoreConfig.getOriginDatabaseName() + "." + System.currentTimeMillis();
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    OdpsDatabaseRestoreAction tableRestoreAction =
        new OdpsDatabaseRestoreAction(taskId + ".Table",
            ObjectType.TABLE,
            restoreConfig,
            mmaMetaManager);
    OdpsDatabaseRestoreAction viewRestoreAction =
        new OdpsDatabaseRestoreAction(taskId + ".View",
            ObjectType.VIEW,
            restoreConfig,
            mmaMetaManager);
    OdpsDatabaseRestoreAction resourceRestoreAction =
        new OdpsDatabaseRestoreAction(taskId + ".Resource",
            ObjectType.RESOURCE,
            restoreConfig,
            mmaMetaManager);
    OdpsDatabaseRestoreAction functionRestoreAction =
        new OdpsDatabaseRestoreAction(taskId + ".Function",
            ObjectType.FUNCTION,
            restoreConfig,
            mmaMetaManager);
    OdpsDatabaseRestoreDeleteMetaAction deleteMetaAction =
        new OdpsDatabaseRestoreDeleteMetaAction(taskId + ".DeleteMeta",
            restoreConfig.getTaskName(),
            mmaMetaManager);

    dag.addVertex(tableRestoreAction);
    dag.addVertex(viewRestoreAction);
    dag.addVertex(resourceRestoreAction);
    dag.addVertex(functionRestoreAction);
    dag.addVertex(deleteMetaAction);

    dag.addEdge(tableRestoreAction, viewRestoreAction);
    dag.addEdge(viewRestoreAction, resourceRestoreAction);
    dag.addEdge(resourceRestoreAction, functionRestoreAction);
    dag.addEdge(functionRestoreAction, deleteMetaAction);

    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateFunctionRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                           TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreFunctionAction action = new OdpsRestoreFunctionAction(taskId + ".RestoreFunction", restoreConfig);
    dag.addVertex(action);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateResourceRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig,
                                           TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    OdpsRestoreResourceAction action = new OdpsRestoreResourceAction(taskId + ".RestoreFunction", restoreConfig);
    dag.addVertex(action);
    return new ObjectExportAndRestoreTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getHiveNonPartitionedTableMigrationActionDag(String taskId) {
    OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");
    OdpsCreateTableAction createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    HiveUdtfDataTransferAction dataTransferAction =
        new HiveUdtfDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    HiveSourceVerificationAction sourceVerificationAction =
        new HiveSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    dag.addVertex(dropTableAction);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(dropTableAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getHivePartitionedTableMigrationActionDag(
      String taskId) {

    OdpsCreateTableAction createTableAction =
        new OdpsCreateTableAction(taskId + ".CreateTable");
    OdpsDropPartitionAction dropPartitionAction =
        new OdpsDropPartitionAction(taskId + ".DropPartition");
    OdpsAddPartitionAction addPartitionAction =
        new OdpsAddPartitionAction(taskId + ".AddPartition");
    HiveUdtfDataTransferAction dataTransferAction =
        new HiveUdtfDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    HiveSourceVerificationAction sourceVerificationAction =
        new HiveSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    dag.addVertex(createTableAction);
    dag.addVertex(dropPartitionAction);
    dag.addVertex(addPartitionAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(createTableAction, dropPartitionAction);
    dag.addEdge(dropPartitionAction, addPartitionAction);
    dag.addEdge(addPartitionAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsNonPartitionedTableMigrationActionDag(String taskId) {
    return getOdpsNonPartitionedTableMigrationActionDag(taskId, false, null);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsNonPartitionedTableMigrationActionDag(
      String taskId, boolean toExternalTable, ExternalTableConfig externalTableConfig) {

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          createTableAction =
              new OdpsCreateOssExternalTableAction(taskId + ".CreateExternalTable", externalTableConfig.getLocation());
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    }

    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    dag.addVertex(dropTableAction);
    dag.addVertex(createTableAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(dropTableAction, createTableAction);
    dag.addEdge(createTableAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsPartitionedTableMigrationActionDag(String taskId) {
    return getOdpsPartitionedTableMigrationActionDag(taskId, false, null);
  }

  private DirectedAcyclicGraph<Action, DefaultEdge> getOdpsPartitionedTableMigrationActionDag(
      String taskId, boolean toExternalTable, ExternalTableConfig externalTableConfig) {

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          createTableAction =
              new OdpsCreateOssExternalTableAction(taskId + ".CreateExternalTable", externalTableConfig.getLocation());
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    }

    Action addPartitionAction;
    if (toExternalTable) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          addPartitionAction =
              new OdpsAddPartitionAction(taskId + ".AddExternalPartition");
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      addPartitionAction = new OdpsAddPartitionAction(taskId + ".AddPartition");
    }

    OdpsInsertOverwriteDataTransferAction dataTransferAction =
        new OdpsInsertOverwriteDataTransferAction(taskId + ".DataTransfer");
    OdpsDestVerificationAction destVerificationAction =
        new OdpsDestVerificationAction(taskId + ".DestVerification");
    OdpsSourceVerificationAction sourceVerificationAction =
        new OdpsSourceVerificationAction(taskId + ".SourceVerification");
    VerificationAction verificationAction = new VerificationAction(taskId + ".Compare");

    dag.addVertex(createTableAction);
    dag.addVertex(addPartitionAction);
    dag.addVertex(dataTransferAction);
    dag.addVertex(destVerificationAction);
    dag.addVertex(sourceVerificationAction);
    dag.addVertex(verificationAction);

    dag.addEdge(createTableAction, addPartitionAction);
    dag.addEdge(addPartitionAction, dataTransferAction);
    dag.addEdge(dataTransferAction, destVerificationAction);
    dag.addEdge(dataTransferAction, sourceVerificationAction);
    dag.addEdge(destVerificationAction, verificationAction);
    dag.addEdge(sourceVerificationAction, verificationAction);

    return dag;
  }

  private List<TableMetaModel> getTableSplits(
      TableMetaModel tableMetaModel,
      MmaConfig.AdditionalTableConfig config) {

    if (tableMetaModel.partitionColumns.isEmpty()) {
      // Splitting non-partitioned tables not supported
      return Collections.singletonList(tableMetaModel);
    }

    List<TableMetaModel> ret = new LinkedList<>();

    // TODO: adaptive table split
    int partitionGroupSize;
    if (config != null && config.getPartitionGroupSize() > 0) {
      partitionGroupSize = config.getPartitionGroupSize();
    } else {
      partitionGroupSize = Math.min(tableMetaModel.partitions.size(), Constants.DEFAULT_PARTITION_BATCH_SIZE);
    }

    int startIdx = 0;

    while (startIdx < tableMetaModel.partitions.size()) {
      TableMetaModel clone = tableMetaModel.clone();

      // Set partitions
      int endIdx = Math.min(tableMetaModel.partitions.size(), startIdx + partitionGroupSize);
      clone.partitions = new ArrayList<>(tableMetaModel.partitions.subList(startIdx, endIdx));
      ret.add(clone);

      startIdx += partitionGroupSize;
    }

    return ret;
  }

  private String getUniqueMigrationTaskName(String db, String tbl) {
    // TODO: better task id generator
    StringBuilder sb = new StringBuilder("Migration.");
    sb.append(db).append(".").append(tbl).append(".").append(System.currentTimeMillis() / 1000);

    return sb.toString();
  }
}
