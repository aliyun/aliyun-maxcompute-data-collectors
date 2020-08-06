package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.BackgroundLoopManager;
import com.aliyun.odps.datacarrier.taskscheduler.BackgroundWorkItem;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

public class AddBackgroundWorkItemAction extends OdpsNoSqlAction {
  private BackgroundLoopManager manager;
  private BackgroundWorkItem item;

  public AddBackgroundWorkItemAction(String id,
                                     BackgroundLoopManager manager,
                                     BackgroundWorkItem item) {
    super(id);
    this.manager = manager;
    this.item = item;
  }

  @Override
  public void doAction() throws MmaException {
    manager.addWorkItem(item);
  }
}
