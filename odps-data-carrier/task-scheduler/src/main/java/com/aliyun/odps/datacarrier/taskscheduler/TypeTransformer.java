package com.aliyun.odps.datacarrier.taskscheduler;

public interface TypeTransformer {

  public TypeTransformResult toOdpsTypeV1(String type);

  public TypeTransformResult toOdpsTypeV2(String type);
}
