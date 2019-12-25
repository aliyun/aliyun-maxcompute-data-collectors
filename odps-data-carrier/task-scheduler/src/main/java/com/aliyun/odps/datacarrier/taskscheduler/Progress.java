package com.aliyun.odps.datacarrier.taskscheduler;

public enum Progress {
  NEW,
  RUNNING,
  SUCCEEDED,
  FAILED;

  public String toColorString() {
    switch (this) {
      case SUCCEEDED:
        return ColorsGenerator.printGreen(this.name());
      case FAILED:
        return ColorsGenerator.printRed(this.name());
      case RUNNING:
        return ColorsGenerator.printYellow(this.name());
      case NEW:
        return ColorsGenerator.printWhite(this.name());
    }
    return name();
  }
}
