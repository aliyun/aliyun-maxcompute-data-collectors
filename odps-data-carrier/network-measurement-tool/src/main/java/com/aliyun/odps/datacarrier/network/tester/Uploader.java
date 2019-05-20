package com.aliyun.odps.datacarrier.network.tester;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import java.util.concurrent.Callable;

public class Uploader implements Callable<Object> {
  private RecordWriter writer;
  private Record record;
  private int numRecord;
  private ConcurrentProgressBar progressBar;

  public Uploader(RecordWriter writer, Record record, int numRecord,
      ConcurrentProgressBar progressBar) {
    this.writer = writer;
    this.record = record;
    this.numRecord = numRecord;
    this.progressBar = progressBar;
  }

  @Override
  public Object call() throws Exception {
    for (int i = 0; i < numRecord; i++) {
      writer.write(record);
      progressBar.step();
    }
    ((TunnelBufferedWriter) writer).flush();
    writer.close();
    return null;
  }
}
