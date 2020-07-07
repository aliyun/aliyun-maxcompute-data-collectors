package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.Objects;

public class MetaBackupTask extends AbstractTask {
  private MetaSource.TableMetaModel tableMetaModel;

  public MetaBackupTask(String id,
                        MetaSource.TableMetaModel tableMetaModel,
                        DirectedAcyclicGraph<Action, DefaultEdge> dag,
                        MmaMetaManager mmaMetaManager) {
    super(id, dag, mmaMetaManager);
    this.tableMetaModel = Objects.requireNonNull(tableMetaModel);
    actionExecutionContext.setTableMetaModel(this.tableMetaModel);
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.SUCCEEDED.equals(progress)) {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
          tableMetaModel.tableName,
          MmaMetaManager.MigrationStatus.SUCCEEDED);
    } else if (TaskProgress.FAILED.equals(progress)) {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
          tableMetaModel.tableName,
          MmaMetaManager.MigrationStatus.FAILED);
    }
  }
}
