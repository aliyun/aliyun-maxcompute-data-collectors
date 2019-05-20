package com.aliyun.odps.datacarrier.network.tester;

import com.aliyun.odps.data.RecordReader;
import java.util.concurrent.Callable;

public class Downloader implements Callable<Object> {
  private RecordReader reader;
  private int numRecord;
  private ConcurrentProgressBar progressBar;

  public Downloader(RecordReader reader, int numRecord, ConcurrentProgressBar progressBar) {
    this.reader = reader;
    this.numRecord = numRecord;
    this.progressBar = progressBar;
  }

  @Override
  public Object call() throws Exception {
    for (int i = 0; i < numRecord; i++) {
      reader.read();
      progressBar.step();
    }
    reader.close();
    return null;
  }
}
