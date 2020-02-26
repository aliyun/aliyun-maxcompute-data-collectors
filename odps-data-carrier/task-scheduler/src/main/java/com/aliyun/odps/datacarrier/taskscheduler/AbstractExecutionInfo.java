package com.aliyun.odps.datacarrier.taskscheduler;

import java.nio.file.Path;
import java.util.Map;

abstract class AbstractExecutionInfo {
  protected Progress progress = Progress.NEW;
  // scriptMode, specify the way to get sql statements,
  // if scriptMode = true, sql statements will read content from file of sqlPath,
  // otherwise sql statements is as sqlStatements.
  private boolean scriptMode;
  private Path sqlPath;
  private String sqlStatements;
  private String result;
  private Map<String, String> multiRecordResult;

  public AbstractExecutionInfo(Path sqlPath) {
    this.scriptMode = true;
    this.sqlPath = sqlPath;
  }

  public AbstractExecutionInfo(String sqlStatements) {
    this.scriptMode = false;
    this.sqlStatements = sqlStatements;
  }

  public AbstractExecutionInfo() {
    this.scriptMode = false;
  }

  public Path getSqlPath() {
    return sqlPath;
  }

  public String getSqlStatements() {
    return sqlStatements;
  }

  public String getResult() {
    return result;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public Map<String, String> getMultiRecordResult() {
    return multiRecordResult;
  }

  public void setMultiRecordResult(Map<String, String> multiRecordResult) {
    this.multiRecordResult = multiRecordResult;
  }

  public boolean isScriptMode() {
    return scriptMode;
  }
}
