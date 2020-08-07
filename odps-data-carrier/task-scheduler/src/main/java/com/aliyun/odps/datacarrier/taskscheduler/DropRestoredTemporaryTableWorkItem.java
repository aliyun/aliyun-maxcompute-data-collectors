package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils;
import com.aliyun.odps.datacarrier.taskscheduler.task.ObjectExportAndRestoreTask;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.Objects;

public class DropRestoredTemporaryTableWorkItem extends BackgroundWorkItem {
  private static final Logger LOG = LogManager.getLogger(DropRestoredTemporaryTableWorkItem.class);

  private String id;
  private String db;
  private String tbl;
  private String taskName;
  private MmaMetaManagerDbImplUtils.RestoreTaskInfo restoreTaskInfo;
  private MetaSource.TableMetaModel tableMetaModel;
  private MmaMetaManager mmaMetaManager;
  private TaskProvider provider;

  private boolean finished = false;

  public DropRestoredTemporaryTableWorkItem(String id,
                                            String db,
                                            String tbl,
                                            String taskName,
                                            MetaSource.TableMetaModel tableMetaModel,
                                            MmaMetaManagerDbImplUtils.RestoreTaskInfo restoreTaskInfo,
                                            MmaMetaManager mmaMetaManager,
                                            TaskProvider provider) {
    this.id = Objects.requireNonNull(id);
    this.db = Objects.requireNonNull(db);
    this.tbl = Objects.requireNonNull(tbl);
    this.taskName = Objects.requireNonNull(taskName);
    this.restoreTaskInfo = restoreTaskInfo;
    this.tableMetaModel = tableMetaModel;
    this.mmaMetaManager = mmaMetaManager;
    this.provider = provider;
  }

  @Override
  public void execute() {
    try {
      MmaMetaManagerDbImplUtils.JobInfo jobInfo = mmaMetaManager.getMigrationJob(db, tbl);
      if (jobInfo == null) {
        LOG.info("db {}, tbl {} not found in meta db", db, tbl);
        finished = true;
        return;
      }
      MmaMetaManager.MigrationStatus status = jobInfo.getStatus();
      if (MmaMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
        LOG.info("Migration {}.{} in restore task {} succeed", db, tbl, taskName);
        OdpsDropTableAction dropTableAction = new OdpsDropTableAction(id, db, tbl, false);
        DirectedAcyclicGraph<Action, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
        dag.addVertex(dropTableAction);
        ObjectExportAndRestoreTask task = new ObjectExportAndRestoreTask(id, tableMetaModel, dag, mmaMetaManager);
        task.setRestoreTaskInfo(restoreTaskInfo);
        provider.addPendingTask(task);
        finished = true;
      } else if (MmaMetaManager.MigrationStatus.FAILED.equals(status)) {
        LOG.info("Migration {}.{} in restore task {} failed", db, tbl, taskName);
        finished = true;
      }
    } catch (MmaException e) {
      LOG.error("Exception when get job status for db {}, tbl {} ", db, tbl, e);
      finished = true;
    }
  }

  @Override
  public boolean finished() {
    return finished;
  }
}
