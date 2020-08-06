package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.Objects;
import java.util.stream.Collectors;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventManager;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaJobFailedEvent;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaJobSuccceedEvent;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.action.ActionProgress;
import com.aliyun.odps.datacarrier.taskscheduler.action.VerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.VerificationActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;

public class MigrationTask extends AbstractTask {

  private TableMetaModel tableMetaModel;

  public MigrationTask(
      String id,
      MetaSource.TableMetaModel tableMetaModel,
      DirectedAcyclicGraph<Action, DefaultEdge> dag,
      MmaMetaManager mmaMetaManager) {
    super(id, dag, mmaMetaManager);
    actionExecutionContext.setTableMetaModel(Objects.requireNonNull(tableMetaModel));
  }

  @Override
  void updateMetadata() throws MmaException {
    this.tableMetaModel = actionExecutionContext.getTableMetaModel();
    if (!tableMetaModel.partitionColumns.isEmpty()) {
      if (TaskProgress.SUCCEEDED.equals(progress)) {
        mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                    tableMetaModel.tableName,
                                    tableMetaModel.partitions
                                        .stream()
                                        .map(p -> p.partitionValues)
                                        .collect(Collectors.toList()),
                                    MmaMetaManager.MigrationStatus.SUCCEEDED);
        MmaJobSuccceedEvent e = new MmaJobSuccceedEvent(tableMetaModel.databaseName,
                                                        tableMetaModel.tableName);
        MmaEventManager.getInstance().send(e);
      } else if (TaskProgress.FAILED.equals(progress)) {
        MmaJobFailedEvent e = new MmaJobFailedEvent(tableMetaModel.databaseName,
                                                      tableMetaModel.tableName);
        MmaEventManager.getInstance().send(e);

        // Update the status of partition who have passed the verification to SUCCEEDED even when
        // the task failed
        Action verificationAction = null;
        for (Action action : dag.vertexSet()) {
          if (action instanceof VerificationAction) {
            verificationAction = action;
          }
        }

        if (verificationAction != null
            && ActionProgress.FAILED.equals(verificationAction.getProgress())) {
          VerificationActionInfo verificationActionInfo =
              (VerificationActionInfo) verificationAction.getActionInfo();
          mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                      tableMetaModel.tableName,
                                      verificationActionInfo.getSucceededPartitions(),
                                      MmaMetaManager.MigrationStatus.SUCCEEDED);
          mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                      tableMetaModel.tableName,
                                      verificationActionInfo.getFailedPartitions(),
                                      MmaMetaManager.MigrationStatus.FAILED);
        } else {
          mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                      tableMetaModel.tableName,
                                      tableMetaModel.partitions
                                          .stream()
                                          .map(p -> p.partitionValues)
                                          .collect(Collectors.toList()),
                                      MmaMetaManager.MigrationStatus.FAILED);
        }
      }
    } else {
      if (TaskProgress.SUCCEEDED.equals(progress)) {
        MmaJobSuccceedEvent e = new MmaJobSuccceedEvent(tableMetaModel.databaseName,
                                                        tableMetaModel.tableName);
        MmaEventManager.getInstance().send(e);

        mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                    tableMetaModel.tableName,
                                    MmaMetaManager.MigrationStatus.SUCCEEDED);
      } else if (TaskProgress.FAILED.equals(progress)) {
        MmaJobFailedEvent e = new MmaJobFailedEvent(tableMetaModel.databaseName,
                                                    tableMetaModel.tableName);
        MmaEventManager.getInstance().send(e);

        mmaMetaManager.updateStatus(tableMetaModel.databaseName,
                                    tableMetaModel.tableName,
                                    MmaMetaManager.MigrationStatus.FAILED);
      }
    }
  }
}
