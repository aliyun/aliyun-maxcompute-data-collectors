package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.Resource;

public class OdpsResourceInfo {

  private String alias;
  private Resource.Type type;
  private String comment;
  private String tableName;
  private String partitionSpec;

  public OdpsResourceInfo(String alias, Resource.Type type, String comment, String tableName, String partitionSpec) {
    this.alias = alias;
    this.type = type;
    this.comment = comment;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  public Resource.Type getType() {
    return type;
  }

  public String getAlias() {
    return alias;
  }

  public String getComment() {
    return comment;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionSpec() {
    return partitionSpec;
  }

}
