package com.aliyun.odps.datacarrier.network.summary;

import com.aliyun.odps.datacarrier.network.Endpoint;
import com.aliyun.odps.datacarrier.network.summary.AbstractSummary;

public class AvailabilitySummary extends AbstractSummary {
  Boolean available;
  long elapsedTime;

  public AvailabilitySummary(Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public Boolean getAvailable() {
    return available;
  }

  public void setAvailable(boolean available) {
    this.available = available;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  @Override
  public void print() {
    System.out.println("------------------------------------------------------------------------");
    System.out.println("  ENDPOINT: " + this.endpoint.toString());
    System.out.println("  AVAILABILITY: " + this.available);
    System.out.println("  ELAPSED TIME (ms): " + this.elapsedTime);
  }
}
