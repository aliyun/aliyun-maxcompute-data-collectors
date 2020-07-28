package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportTableDDLAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportViewAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestorePartitionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsRestoreTableAction;
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

  public TaskProvider(MmaMetaManager mmaMetaManager) {
    this.mmaMetaManager = Objects.requireNonNull(mmaMetaManager);
  }

  public synchronized List<Task> get() throws MmaException {

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
          case TABLE:
          case VIEW: {
            task = generateTableExportTask(taskName, tableMetaModel);
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
        MmaConfig.ObjectRestoreConfig restoreConfig = MmaConfig.ObjectRestoreConfig.fromJson(config.getDescription());
        Task task = null;
        switch (restoreConfig.getObjectType()) {
          case FUNCTION:
            task = generateFunctionRestoreTask(restoreConfig, tableMetaModel);
            break;
          case RESOURCE:
            task = generateResourceRestoreTask(restoreConfig, tableMetaModel);
            break;
          case TABLE:
          case VIEW:
            task = generateTableRestoreTask(restoreConfig, tableMetaModel);
            break;
          default:
            LOG.error("Unsupported restore type {}", config.getDescription());
        }
        ret.add(task);
      } else {
        LOG.error("Unsupported job type {} for {}.{}", jobType, tableMetaModel.databaseName, tableMetaModel.tableName);
        // TODO: should mark corresponding job as failed
      }
    }

    return ret;
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
          dag = getOdpsNonPartitionedTableMigrationActionDag(taskId, false, null);
        }
        break;
      }
      case OSS:
      default:
        throw new UnsupportedOperationException();
    }

    return new MigrationTask(taskId, tableMetaModel, config, dag, mmaMetaManager);
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
      Task task = new MigrationTask(taskId, tableMetaModel, config, dag, mmaMetaManager);
      ret.add(task);
      return ret;
    }

    String taskNamePrefix = getUniqueMigrationTaskName(
        tableMetaModel.databaseName,
        tableMetaModel.tableName);
    List<TableMetaModel> tableSplits = getTableSplits(tableMetaModel, config);

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
            dag = getOdpsPartitionedTableMigrationActionDag(taskId, false, null);
          }
          break;
        }
        case OSS:
        default:
          throw new UnsupportedOperationException();
      }

      ret.add(new MigrationTask(taskId, split, config, dag, mmaMetaManager));
    }

    return ret;
  }

  private Task generateTableExportTask(String taskName, TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    Table table = OdpsUtils.getTable(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (table == null) {
      LOG.info("Table {}.{} not found", tableMetaModel.databaseName, tableMetaModel.tableName);
      return null;
    }
    DirectedAcyclicGraph<Action, DefaultEdge> dag = null;
    if (table.isVirtualView()) {
      dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      OdpsExportViewAction action = new OdpsExportViewAction(taskId + ".ExportViewDDL", taskName, table.getViewText());
      dag.addVertex(action);
    } else {
      String location = OssUtils.getOssPathToExportObject(
          taskName,
          Constants.EXPORT_TABLE_FOLDER,
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          Constants.EXPORT_TABLE_DATA_FOLDER);
      MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
      ExternalTableConfig externalTableConfig = new ExternalTableConfig(ExternalTableStorage.OSS,
          OdpsSqlUtils.getOssTablePath(ossConfig.getOssEndpoint(), ossConfig.getOssBucket(), location));
      if (tableMetaModel.partitionColumns.isEmpty()) {
        dag = getOdpsNonPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
      } else {
        dag = getOdpsPartitionedTableMigrationActionDag(taskId, true, externalTableConfig);
      }
      Action leafAction = null;
      for(Action action : dag.vertexSet()) {
        if (dag.inDegreeOf(action) > 0 && dag.outDegreeOf(action) == 0) {
          leafAction = action;
          break;
        }
      }
      OdpsExportTableDDLAction exportTableDDLAction = new OdpsExportTableDDLAction(taskId + ".ExportTableDDL", taskName);
      dag.addVertex(exportTableDDLAction);
      OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");
      dag.addVertex(dropTableAction);
      dag.addEdge(leafAction, exportTableDDLAction);
      dag.addEdge(exportTableDDLAction, dropTableAction);
    }

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

  private Task generateTableRestoreTask(MmaConfig.ObjectRestoreConfig restoreConfig, TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.odpsProjectName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    String taskName = restoreConfig.getTaskName();
    MmaConfig.ObjectType type = restoreConfig.getObjectType();
    OdpsRestoreTableAction action = new OdpsRestoreTableAction(
        taskId + ".Restore" + type.name(),
        taskName,
        tableMetaModel.databaseName,
        tableMetaModel.odpsProjectName,
        tableMetaModel.tableName,
        type,
        restoreConfig.getSettings());
    dag.addVertex(action);
    if (restoreConfig.isUpdate()) {
      OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable",
          MmaConfig.ObjectType.VIEW.equals(type));
      dag.addVertex(dropTableAction);
      dag.addEdge(dropTableAction, action);
    }
    if (MmaConfig.ObjectType.TABLE.equals(type)) {
      String ossFileName = OssUtils.getOssPathToExportObject(taskName,
          EXPORT_TABLE_FOLDER,
          tableMetaModel.databaseName,
          tableMetaModel.tableName,
          EXPORT_PARTITION_SPEC_FILE_NAME);
      if (OssUtils.exists(ossFileName)) {
        OdpsRestorePartitionAction restorePartitionAction = new OdpsRestorePartitionAction(
            taskId + ".RestorePartition",
            taskName,
            tableMetaModel.databaseName,
            tableMetaModel.odpsProjectName,
            tableMetaModel.tableName,
            restoreConfig.getSettings());
        dag.addVertex(restorePartitionAction);
        dag.addEdge(action, restorePartitionAction);
      } else {
        LOG.info("PartitionSpec file not found for {}.{}", tableMetaModel.databaseName, tableMetaModel.tableName);
      }
    }
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

  private DirectedAcyclicGraph<Action, DefaultEdge> getHiveNonPartitionedTableMigrationActionDag(
      String taskId) {

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
      TableMigrationConfig config) {

    if (tableMetaModel.partitionColumns.isEmpty()) {
      // Splitting non-partitioned tables not supported
      return Collections.singletonList(tableMetaModel);
    }

    List<TableMetaModel> ret = new LinkedList<>();

    // TODO: adaptive table split
    int partitionGroupSize;
    if (config != null && config.getAdditionalTableConfig().getPartitionGroupSize() > 0) {
      partitionGroupSize = config.getAdditionalTableConfig().getPartitionGroupSize();
    } else {
      partitionGroupSize = Math.min(tableMetaModel.partitions.size(), 1000);
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
