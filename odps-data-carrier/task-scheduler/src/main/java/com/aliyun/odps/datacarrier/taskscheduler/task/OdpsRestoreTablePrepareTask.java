package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.Objects;

public class OdpsRestoreTablePrepareTask extends ObjectExportAndRestoreTask {

  private final MetaSource.TableMetaModel tableMetaModel;

  public OdpsRestoreTablePrepareTask(String id,
                                    MetaSource.TableMetaModel tableMetaModel,
                                    DirectedAcyclicGraph<Action, DefaultEdge> dag,
                                    MmaMetaManager mmaMetaManager) {
    super(id, tableMetaModel, dag, mmaMetaManager);
    this.tableMetaModel = Objects.requireNonNull(tableMetaModel);
    actionExecutionContext.setTableMetaModel(this.tableMetaModel);
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.PENDING.equals(progress) ||
        TaskProgress.RUNNING.equals(progress) ||
        TaskProgress.SUCCEEDED.equals(progress)) {
      return;
    }
    MmaMetaManager.MigrationStatus status = MmaMetaManager.MigrationStatus.FAILED;
    MmaMetaManagerDbImplUtils.RestoreTaskInfo restoreTaskInfo = getRestoreTaskInfo();
    if (restoreTaskInfo != null) {
      mmaMetaManager.updateStatusInRestoreDB(restoreTaskInfo, status);
    } else {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
          tableMetaModel.tableName,
          status);
    }
  }
}
