package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.Map;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import org.h2.util.StringUtils;

public class OdpsDropTableAction extends OdpsSqlAction {

  private String db;
  private String tbl;
  private boolean isView = false;

  public OdpsDropTableAction(String id) {
    super(id);
  }

  public OdpsDropTableAction(String id, boolean isView) {
    this(id);
    this.isView = isView;
  }

  public OdpsDropTableAction(String id, String db, String tbl, boolean isView) {
    this(id);
    this.db = db;
    this.tbl = tbl;
    this.isView = isView;
  }

  @Override
  String getSql() {
    if (StringUtils.isNullOrEmpty(db) || StringUtils.isNullOrEmpty(tbl)) {
      MetaSource.TableMetaModel tableMetaModel = actionExecutionContext.getTableMetaModel();
      db = tableMetaModel.odpsProjectName;
      tbl = tableMetaModel.odpsTableName;
    }
    if (isView) {
      return OdpsSqlUtils.getDropViewStatement(db, tbl);
    }
    return OdpsSqlUtils.getDropTableStatement(db, tbl);
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
