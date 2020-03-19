package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.data.Record;

import java.util.LinkedList;
import java.util.List;

public class OdpsActionInfo extends AbstractActionInfo {
  private List<OdpsExecutionInfo> infos = new LinkedList<>();

  static class OdpsExecutionInfo {
    private String instanceId;
    private String logView;
    private List<Record> result;

    public void setInstanceId(String instanceId) {
      this.instanceId = instanceId;
    }

    public void setLogView(String logView) {
      this.logView = logView;
    }

    public void setResult(List<Record> result) {
      this.result = result;
    }

    public List<Record> getResult() {
      return result;
    }
  }

  public List<OdpsExecutionInfo> getInfos() {
    return infos;
  }

  public void addInfo(OdpsExecutionInfo info) {
    this.infos.add(info);
  }

  public String getOdpsActionInfoSummary() {
    final StringBuilder sb = new StringBuilder();
    for (OdpsExecutionInfo info : infos) {
      sb.append("\nInstanceId= ").append(info.instanceId);
      sb.append("\nLogView= ").append(info.logView);
      sb.append("\n");
    }
    return sb.toString();
  }
}
