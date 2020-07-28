package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;

public class OdpsFunctionInfo {
  private String functionName;
  private String className;
  private List<String> useList;

  public OdpsFunctionInfo(String functionName, String className, List<String> useList) {
    this.functionName = functionName;
    this.className = className;
    this.useList = useList;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public List<String> getUseList() {
    return useList;
  }

}
