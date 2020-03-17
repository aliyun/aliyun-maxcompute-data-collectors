package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.ArrayList;
import java.util.List;

public class ValidateExecutionInfo extends AbstractExecutionInfo{
  HiveExecutionInfo hiveExecutionInfo = new HiveExecutionInfo();
  OdpsExecutionInfo odpsExecutionInfo = new OdpsExecutionInfo();
  List<List<String>> succeededPartitionValuesList = new ArrayList<>();


}
