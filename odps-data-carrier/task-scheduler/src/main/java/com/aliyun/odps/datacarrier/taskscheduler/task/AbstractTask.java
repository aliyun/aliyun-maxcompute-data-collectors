/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.ColorsGenerator;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.action.AbstractAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.ActionProgress;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;


public abstract class AbstractTask implements Task {
  private static final Logger LOG = LogManager.getLogger(AbstractTask.class);

  String id;
  TaskProgress progress = TaskProgress.PENDING;
  DirectedAcyclicGraph<Action, DefaultEdge> dag;
  ActionExecutionContext actionExecutionContext = new ActionExecutionContext();
  private ActionProgressListener actionProgressListener = new ActionProgressListener(this);

  MmaMetaManager mmaMetaManager;

  public AbstractTask(
      String id,
      DirectedAcyclicGraph<Action, DefaultEdge> dag,
      MmaMetaManager mmaMetaManager) {

    this.id = Objects.requireNonNull(id);
    this.dag = Objects.requireNonNull(dag);
    this.mmaMetaManager = Objects.requireNonNull(mmaMetaManager);

    setActionProgressListener();
    setActionExecutionContext();
  }

  private void setActionProgressListener() {
    dag.vertexSet()
       .forEach(v -> ((AbstractAction) v).setActionProgressListener(actionProgressListener));
  }

  private void setActionExecutionContext() {
    dag.vertexSet()
       .forEach(v -> ((AbstractAction) v).setActionExecutionContext(actionExecutionContext));
  }

  @Override
  public TaskProgress getProgress() {
    return progress;
  }

  /**
   * Return executable actions.
   * @return executable actions.
   */
  @Override
  public List<Action> getExecutableActions() {
    List<Action> ret = new LinkedList<>();

    for (Action a : dag.vertexSet()) {
      boolean executable = ActionProgress.PENDING.equals(a.getProgress())
          && dag.getAncestors(a)
                .stream()
                .allMatch(p -> ActionProgress.SUCCEEDED.equals(p.getProgress()));

      if (executable) {
        ret.add(a);
      }
    }

    if (ret.size() > 0) {
      LOG.info("GetExecutableActions, ret: {}", ret);
    }
    return ret;
  }

  /**
   * Update task progress, triggered by an action progress update
   * @param actionNewProgress the new action progress that triggers this update
   */
  private synchronized void updateTaskProgress(ActionProgress actionNewProgress)
      throws MmaException {

    boolean taskProgressChanged = false;
    TaskProgress currentProgress = progress;

    switch (progress) {
      case PENDING:
        if (ActionProgress.RUNNING.equals(actionNewProgress)) {
          progress = TaskProgress.RUNNING;
          taskProgressChanged = true;
        }
        break;
      case RUNNING:
        if (ActionProgress.FAILED.equals(actionNewProgress)) {
          progress = TaskProgress.FAILED;
          taskProgressChanged = true;
        } else if (ActionProgress.SUCCEEDED.equals(actionNewProgress)) {
          boolean allActionsSucceeded = dag.vertexSet()
              .stream()
              .allMatch(v -> ActionProgress.SUCCEEDED.equals(v.getProgress()));
          if (allActionsSucceeded) {
            progress = TaskProgress.SUCCEEDED;
            taskProgressChanged = true;
          }
        }
        break;
      case FAILED:
      case SUCCEEDED:
      default:
    }

    if (taskProgressChanged) {
      LOG.info("Task {} change progress from {} to {}", id, currentProgress, progress);
      updateMetadata();
    }
  }

  /**
   * Update metadata, triggered by an task progress update
   */
  abstract void updateMetadata() throws MmaException;

  @Override
  public String getId() {
    return id;
  }

  /**
   * Return a simple description of this task
   * @return Description of this task
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(id).append(" ");

    Iterator<Action> iterator = dag.iterator();
    while (iterator.hasNext()) {
      Action a = iterator.next();
      if (ActionProgress.SUCCEEDED.equals(a.getProgress())) {
        sb.append(ColorsGenerator.printGreen(a.getId()));
      } else if (ActionProgress.FAILED.equals(a.getProgress())) {
        sb.append(ColorsGenerator.printRed(a.getId()));
      } else {
        sb.append(ColorsGenerator.printYellow(a.getId()));
      }
      sb.append(" ");
    }

    return sb.toString();
  }

  @Override
  public void stop() {
    // TODO: stop all actions
  }

  public class ActionProgressListener {
    private AbstractTask task;

    public ActionProgressListener(AbstractTask task) {
      this.task = Objects.requireNonNull(task);
    }

    public void onActionProgressChanged(ActionProgress newProgress) throws MmaException {
      task.updateTaskProgress(newProgress);
    }
  }
}
