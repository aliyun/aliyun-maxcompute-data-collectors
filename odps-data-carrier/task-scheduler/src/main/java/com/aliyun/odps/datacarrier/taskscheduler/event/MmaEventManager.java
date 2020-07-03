package com.aliyun.odps.datacarrier.taskscheduler.event;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;

public class MmaEventManager {
  private static final Logger LOG = LogManager.getLogger(MmaEventManager.class);

  private static final int DEFAULT_EVENT_HANDLING_INTERVAL_MS = 1000;

  private static MmaEventManager instance;

  private final List<MmaEventSender> messageSenders =
      Collections.synchronizedList(new LinkedList<>());
  private final Set<MmaEventType> blacklist = new ConcurrentHashSet<>();
  private final Set<MmaEventType> whitelist = new ConcurrentHashSet<>();

  private volatile boolean keepRunning = true;
  private EventHandlingThread eventHandlingThread;
  private Queue<BaseMmaEvent> eventQueue = new ConcurrentLinkedDeque<>();

  private MmaEventManager() {
    eventHandlingThread = new EventHandlingThread();
    eventHandlingThread.start();
  }

  private void sendInternal(BaseMmaEvent e) {
    synchronized (messageSenders) {
      messageSenders.forEach(s -> s.send(e));
    }
  }

  public static MmaEventManager getInstance() {
    if (instance == null) {
      instance = new MmaEventManager();
    }

    return instance;
  }

  public void register(MmaEventSender sender) {
    LOG.info("Register mma event sender: {}", sender.getClass().getName());
    messageSenders.add(Objects.requireNonNull(sender));
  }

  public void blacklist(MmaEventType type) {
    LOG.info("Blacklist mma event type: {}", type.name());
    blacklist.add(type);
  }

  public void whitelist(MmaEventType type) {
    LOG.info("Whitelist mma event type: {}", type.name());
    whitelist.add(type);
  }

  public void send(BaseMmaEvent e) {
    eventQueue.offer(e);
  }

  public void shutdown() {
    keepRunning = false;
    try {
      eventHandlingThread.join();
    } catch (InterruptedException ignore) {
    }
  }

  private class EventHandlingThread extends Thread {
    private int eventHandlingInterval = DEFAULT_EVENT_HANDLING_INTERVAL_MS;

    public EventHandlingThread() {
      super("EventHandling");
    }

    @Override
    public void run() {
      LOG.info("EventHandling thread starts");
      while (keepRunning) {
        while (!eventQueue.isEmpty()) {
          BaseMmaEvent e = eventQueue.poll();
          if (e == null) {
            continue;
          }

          if (!whitelist.isEmpty()) {
            if (whitelist.contains(e.getType())) {
              sendInternal(e);
            }
          } else if (!blacklist.isEmpty()) {
            if (!blacklist.contains(e.getType())) {
              sendInternal(e);
            }
          } else {
             sendInternal(e);
          }
        }

        try {
          Thread.sleep(eventHandlingInterval);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }
}
