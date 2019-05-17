package com.aliyun.odps.datacarrier.network.summary;

import com.aliyun.odps.datacarrier.network.Endpoint;

public class PerformanceSummary extends AbstractSummary {
  /**
   * In MB per second
   */
  long uploadElapsedTime;
  long downloadElapsedTime;

  public PerformanceSummary(Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public void setUploadElapsedTime(long uploadElapsedTime) {
    this.uploadElapsedTime = uploadElapsedTime;
  }

  public void setDownloadElapsedTime(long downloadElapsedTime) {
    this.downloadElapsedTime = downloadElapsedTime;
  }

  public long getUploadElapsedTime() {
    return this.uploadElapsedTime;
  }

  public long getDownloadElapsedTime() {
    return this.downloadElapsedTime;
  }

  @Override
  public void print() {
    System.out.println("------------------------------------------------------------------------");
    System.out.println("  ENDPOINT: " + this.endpoint.toString());
    System.out.println("  UPLOAD PERFORMANCE (MB/s): "
        + 1000.0 * 1000 / this.uploadElapsedTime);
    System.out.println("  DOWNLOAD PERFORMANCE (MB/s): "
        + 1000.0 * 1000 / this.downloadElapsedTime);
  }
}
