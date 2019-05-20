package com.aliyun.odps.datacarrier.network.summary;

import com.aliyun.odps.datacarrier.network.Endpoint;
import com.aliyun.odps.datacarrier.network.tester.PerformanceTester;

public class PerformanceSummary extends AbstractSummary {
  /**
   * In MB per second
   */
  long uploadElapsedTime = Long.MAX_VALUE;
  long downloadElapsedTime = Long.MAX_VALUE;
  float dataSize;

  public PerformanceSummary(Endpoint endpoint) {
    this.endpoint = endpoint;
  }

  public void setDataSizeMegaByte(float dataSize) {
    this.dataSize = dataSize;
  }

  public float getDataSize() {
    return this.dataSize;
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
    System.out.printf("  UPLOAD PERFORMANCE (MB/s): %.2f\n"
        , 1000 * dataSize / this.uploadElapsedTime);
    System.out.printf("  DOWNLOAD PERFORMANCE (MB/s): %.2f\n"
        , 1000 * dataSize / this.downloadElapsedTime);
  }
}
