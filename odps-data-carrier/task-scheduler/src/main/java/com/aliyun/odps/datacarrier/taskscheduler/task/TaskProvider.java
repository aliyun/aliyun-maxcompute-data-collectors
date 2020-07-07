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
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.action.AbstractAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportFunctionAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportResourceAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportTableDDLAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsExportViewDDLAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
import com.aliyun.odps.datacarrier.taskscheduler.ExternalTableStorage;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsAddOssExternalPartitionAction;
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
      if (MmaConfig.JobType.TABLE_MIGRATE.equals(jobType)) {
        TableMigrationConfig tableMigrationConfig = TableMigrationConfig.fromJson(config.getDescription());
        if (tableMetaModel.partitionColumns.isEmpty()) {
          ret.add(generateNonPartitionedTableMigrationTask(datasource, tableMetaModel, tableMigrationConfig));
        } else {
          ret.addAll(generatePartitionedTableMigrationTask(datasource, tableMetaModel, tableMigrationConfig));
        }
      } else if (MmaConfig.JobType.META_BACKUP.equals(jobType)) {
        MmaConfig.ObjectExportConfig backupConf = MmaConfig.ObjectExportConfig.fromJson(config.getDescription());
        switch (backupConf.getMetaType()) {
          case TABLE: {
            Task task = generateTableExportTask(tableMetaModel);
            if (task != null) {
              ret.add(task);
            }
            break;
          }
          case FUNCTION: {
            Task task = generateFunctionExportTask(tableMetaModel);
            if (task != null) {
              ret.add(task);
            }
            break;
          }
          case RESOURCE: {
            Task task = generateResourceExportTask(tableMetaModel);
            if (task != null) {
              ret.add(task);
            }
            break;
          }
          default:
            LOG.error("Unsupported meta type {} when backup {}.{} to OSS",
                backupConf.getMetaType(), backupConf.getDatabaseName(), backupConf.getMetaName());
        }
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
          dag = getOdpsNonPartitionedTableMigrationActionDag(taskId, true, storage);
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
            dag = getOdpsPartitionedTableMigrationActionDag(taskId, true, storage);
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

  private Task generateTableExportTask(TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Table table = OdpsUtils.getTable(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (table == null) {
      LOG.info("Table {}.{} not found", tableMetaModel.databaseName, tableMetaModel.tableName);
      return null;
    }
    AbstractAction action;
    if (table.isVirtualView()) {
      action = new OdpsExportViewDDLAction(taskId + ".ExportViewDDL", table.getViewText());
    } else {
      action = new OdpsExportTableDDLAction(taskId + ".ExportTableDDL");
    }
    dag.addVertex(action);
    return new MetaBackupTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateFunctionExportTask(TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Function function = OdpsUtils.getFunction(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (function == null) {
      return null;
    }
    OdpsExportFunctionAction action = new OdpsExportFunctionAction(taskId + ".ExportFunctionDDL", function);
    dag.addVertex(action);
    return new MetaBackupTask(taskId, tableMetaModel, dag, mmaMetaManager);
  }

  private Task generateResourceExportTask(TableMetaModel tableMetaModel) {
    String taskId = getUniqueMigrationTaskName(tableMetaModel.databaseName, tableMetaModel.tableName);
    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
    Resource resource = OdpsUtils.getResource(tableMetaModel.databaseName, tableMetaModel.tableName);
    if (resource == null) {
      return null;
    }
    OdpsExportResourceAction action = new OdpsExportResourceAction(taskId + ".ExportResourceDDL", resource);
    dag.addVertex(action);
    return new MetaBackupTask(taskId, tableMetaModel, dag, mmaMetaManager);
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
      String taskId, boolean toExternalTable, ExternalTableStorage externalTableStorage) {

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    OdpsDropTableAction dropTableAction = new OdpsDropTableAction(taskId + ".DropTable");

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableStorage) {
        case OSS:
          createTableAction =
              new OdpsCreateOssExternalTableAction(taskId + ".CreateExternalTable");
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
      String taskId, boolean toExternalTable, ExternalTableStorage externalTableStorage) {

    DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);

    Action createTableAction;
    if (toExternalTable) {
      switch (externalTableStorage) {
        case OSS:
          createTableAction =
              new OdpsCreateOssExternalTableAction(taskId + ".CreateExternalTable");
          break;
        default:
          throw new IllegalArgumentException("Unsupported external table storage");
      }
    } else {
      createTableAction = new OdpsCreateTableAction(taskId + ".CreateTable");
    }

    Action addPartitionAction;
    if (toExternalTable) {
      switch (externalTableStorage) {
        case OSS:
          addPartitionAction =
              new OdpsAddOssExternalPartitionAction(taskId + ".AddExternalPartition");
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
