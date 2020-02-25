package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.metaprocessor.TypeTransformResult;

public interface TypeTransformer {

  public TypeTransformResult toOdpsTypeV1(String type);

  public TypeTransformResult toOdpsTypeV2(String type);
}
