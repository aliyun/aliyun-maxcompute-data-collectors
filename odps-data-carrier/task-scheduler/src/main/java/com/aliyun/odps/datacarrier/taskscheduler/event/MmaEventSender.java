package com.aliyun.odps.datacarrier.taskscheduler.event;

public interface MmaEventSender {
  void send(BaseMmaEvent e);
}
