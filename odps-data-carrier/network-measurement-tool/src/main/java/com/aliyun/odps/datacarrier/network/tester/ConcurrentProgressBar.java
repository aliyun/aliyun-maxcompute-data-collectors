package com.aliyun.odps.datacarrier.network.tester;

import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class ConcurrentProgressBar {
  private boolean isClosed = false;
  private ProgressBar progressBar;

  public ConcurrentProgressBar(int max) {
    ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
    progressBarBuilder.setInitialMax(max);
    progressBarBuilder.setStyle(ProgressBarStyle.ASCII);
    progressBar = progressBarBuilder.build();
  }

  public synchronized void step() {
    progressBar.step();
  }

  public synchronized void close() {
    if (!isClosed) {
      isClosed = true;
      progressBar.close();
    }
  }
}
