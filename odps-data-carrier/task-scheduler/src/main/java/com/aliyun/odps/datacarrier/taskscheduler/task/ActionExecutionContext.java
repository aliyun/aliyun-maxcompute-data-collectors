package com.aliyun.odps.datacarrier.taskscheduler.task;

import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;

/**
 * Should support Multi-threading
 */
public class ActionExecutionContext {

  private TableMetaModel tableMetaModel = null;
  private List<List<String>> sourceVerificationResult = null;
  private List<List<String>> destVerificationResult = null;

  public TableMetaModel getTableMetaModel() {
    return tableMetaModel;
  }

  public void setTableMetaModel(TableMetaModel tableMetaModel) {
    this.tableMetaModel = tableMetaModel;
  }

  public List<List<String>> getSourceVerificationResult() {
    return sourceVerificationResult;
  }

  public void setSourceVerificationResult(List<List<String>> rows) {
    sourceVerificationResult = rows;
  }

  public List<List<String>> getDestVerificationResult() {
    return destVerificationResult;
  }

  public void setDestVerificationResult(List<List<String>> rows) {
    destVerificationResult = rows;
  }
}
