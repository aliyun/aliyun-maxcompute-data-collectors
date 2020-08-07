package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;

public class OdpsInsertOverwriteDataTransferAction extends OdpsSqlAction {

  public OdpsInsertOverwriteDataTransferAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getInsertOverwriteTableStatement(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getSourceTableSettings()
        .getMigrationSettings();
  }
}
