package com.aliyun.odps.datacarrier.taskscheduler.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.HiveSqlUtils;

public class HiveUdtfDataTransferAction extends HiveSqlAction {

  private static final Logger LOG = LogManager.getLogger(HiveUdtfDataTransferAction.class);

  public HiveUdtfDataTransferAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return HiveSqlUtils.getUdtfSql(actionExecutionContext.getTableMetaModel());
  }
}
