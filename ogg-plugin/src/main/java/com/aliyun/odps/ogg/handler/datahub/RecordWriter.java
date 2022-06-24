package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.odps.ogg.handler.datahub.modle.Configure;
import com.aliyun.odps.ogg.handler.datahub.modle.TableMapping;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordWriter {
    private static RecordWriter recordWriter;

    private Configure configure;
    private ExecutorService executor;
    private Map<String, TopicWriter> topicWriterMap;

    private RecordWriter(Configure configure) {
        this.configure = configure;

        int corePoolSize = configure.getWriteRecordCorePoolSize() == -1
                ? configure.getTableMappings().size()
                : configure.getWriteRecordCorePoolSize();
        corePoolSize = Math.max(corePoolSize, 1);
        int maximumPoolSize = configure.getWriteRecordMaximumPoolSize() == -1
                ? corePoolSize * 2
                : configure.getWriteRecordMaximumPoolSize();
        executor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 60L,
                TimeUnit.SECONDS, new SynchronousQueue<>(), new RecordWriterThreadFactory());

        createWriters();
    }

    public static RecordWriter instance() {
        return recordWriter;
    }

    public static void init(Configure configure) {
        if (recordWriter == null) {
            recordWriter = new RecordWriter(configure);
        }

        recordWriter.start();
    }

    public void start() {
        for (TopicWriter writer : topicWriterMap.values()) {
            writer.start();
        }
    }

    public void stop() {
        for (TopicWriter writer : topicWriterMap.values()) {
            writer.stop();
        }
        executor.shutdown();
    }

    public static void destroy() {
        if (recordWriter != null) {
            recordWriter.flushAll();
            recordWriter.stop();
        }
        recordWriter = null;
    }

    public void flushAll() {
        for (TopicWriter writer : topicWriterMap.values()) {
            writer.syncExec();
        }
    }

    public TopicWriter getTopicWriter(String oracleTableName) {
        return topicWriterMap.get(oracleTableName);
    }

    private void createWriters() {
        topicWriterMap = new HashMap<>();

        for (Map.Entry<String, TableMapping> entry : configure.getTableMappings().entrySet()) {
            TopicWriter topicWriter = new TopicWriter(configure, entry.getValue(), executor);
            topicWriterMap.put(entry.getKey(), topicWriter);
        }
    }

    private static class RecordWriterThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        RecordWriterThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "DataHub-writer-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }

            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
