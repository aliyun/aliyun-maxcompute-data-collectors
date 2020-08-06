package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BackgroundLoopManager {
  private static final Logger LOG = LogManager.getLogger(BackgroundLoopManager.class);

  private final int BACKGROUND_LOOP_INTERVAL_IN_MS = 10000;
  private Thread backgroundLoop;
  private Set<BackgroundWorkItem> items = ConcurrentHashMap.newKeySet();

  public void start() {
    startBackgroundLoop();
  }

  public boolean addWorkItem(BackgroundWorkItem item) {
    return items.add(item);
  }

  private void startBackgroundLoop() {
    LOG.info(this.getClass().getName() + " start background loop");
    this.backgroundLoop = new Thread(this.getClass().getName() + " background thread") {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          Iterator<BackgroundWorkItem> iter = items.iterator();
          while (iter.hasNext()) {
            BackgroundWorkItem item = iter.next();
            item.execute();
            if (item.finished()) {
              iter.remove();
            }
          }
          try {
            Thread.sleep(BACKGROUND_LOOP_INTERVAL_IN_MS);
          } catch (InterruptedException e) {
            LOG.warn("Background loop interrupted ", e);
          }
        }
      }
    };
    this.backgroundLoop.start();
  }
}
