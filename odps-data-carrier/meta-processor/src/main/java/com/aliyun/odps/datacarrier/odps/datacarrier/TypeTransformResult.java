package com.aliyun.odps.datacarrier.odps.datacarrier;

import com.aliyun.odps.datacarrier.commons.Constants.DATASOURCE_TYPE;
import com.aliyun.odps.datacarrier.commons.Risk;

public class TypeTransformResult {

  private DATASOURCE_TYPE datasourceType;
  private String originalType;
  private String transformedType;
  private Risk risk;

  public TypeTransformResult(DATASOURCE_TYPE datasourceType, String originalType,
      String transformedType, Risk risk) {
    this.datasourceType = datasourceType;
    this.originalType = originalType;
    this.transformedType = transformedType;
    this.risk = risk;
  }

  public DATASOURCE_TYPE getDatasourceType() {
    return this.datasourceType;
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
