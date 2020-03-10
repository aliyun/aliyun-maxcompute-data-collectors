package com.aliyun.odps.datacarrier.taskscheduler;


public class TypeTransformResult {

  private DataSource datasource;
  private String originalType;
  private String transformedType;
  private Risk risk;

  public TypeTransformResult(DataSource datasource, String originalType,
                             String transformedType, Risk risk) {
    this.datasource = datasource;
    this.originalType = originalType;
    this.transformedType = transformedType;
    this.risk = risk;
  }

  public DataSource getDatasource() {
    return this.datasource;
  }

  public String getOriginalType() {
    return this.originalType;
  }

  public String getTransformedType() {
    return this.transformedType;
  }

  public void setTransformedType(String transformedType) {
    this.transformedType = transformedType;
  }

  public Risk getRisk() {
    return this.risk;
  }
}
