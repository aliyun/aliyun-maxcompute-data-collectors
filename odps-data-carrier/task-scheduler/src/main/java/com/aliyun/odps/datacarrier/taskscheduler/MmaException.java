package com.aliyun.odps.datacarrier.taskscheduler;

public class MmaException extends Exception {

  public MmaException(String errorMsg, Throwable e) {
    super(errorMsg, e);
  }

  public MmaException(String errMsg) {
    super(errMsg);
  }



}
