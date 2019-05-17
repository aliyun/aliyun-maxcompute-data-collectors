package com.aliyun.odps.datacarrier.network.summary;

import com.aliyun.odps.datacarrier.network.Endpoint;

public abstract class AbstractSummary {
  Endpoint endpoint;

  public Endpoint getEndpoint() {
    return this.endpoint;
  }

  public void setEndpoint(Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public abstract void print();
}
