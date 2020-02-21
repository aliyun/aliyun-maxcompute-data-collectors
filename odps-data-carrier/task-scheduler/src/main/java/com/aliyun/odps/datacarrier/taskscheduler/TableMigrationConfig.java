package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metacarrier.MetaSource;

/**
 * mock
 */
public class TableMigrationConfig {
  public int getFailOverTimesLimit() {
    return 1;
  }

  public void applyRules(MetaSource.TableMetaModel tableMetaModel) {
    tableMetaModel.odpsProjectName = tableMetaModel.databaseName;
    tableMetaModel.odpsTableName = tableMetaModel.tableName;

    for (MetaSource.ColumnMetaModel c : tableMetaModel.columns) {
      c.odpsColumnName = c.columnName;
      c.odpsType = c.type;
    }

    for (MetaSource.ColumnMetaModel pc : tableMetaModel.partitionColumns) {
      pc.odpsColumnName = pc.columnName;
      pc.odpsType = pc.type;
    }
  }

  public static TableMigrationConfig fromString(String str) {
    return new TableMigrationConfig();
  }
}
