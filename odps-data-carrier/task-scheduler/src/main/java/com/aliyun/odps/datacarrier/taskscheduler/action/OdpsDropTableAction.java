package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;

public class OdpsDropTableAction extends OdpsSqlAction {

  private boolean isView = false;

  public OdpsDropTableAction(String id) {
    super(id);
  }

  public OdpsDropTableAction(String id, boolean isView) {
    this(id);
    this.isView = isView;
  }

  @Override
  String getSql() {
    MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
    if (isView) {
      return OdpsSqlUtils.getDropViewStatement(tableMetaModel.odpsProjectName, tableMetaModel.odpsTableName);
    }
    return OdpsSqlUtils.getDropTableStatement(tableMetaModel.odpsProjectName, tableMetaModel.odpsTableName);
  }

  @Override
  Map<String, String> getSettings() {
    // TODO: should be included in TableMigrationCongifg
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }
}
