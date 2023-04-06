package org.apache.flink.odps.sink.upsert;

import com.aliyun.odps.*;
import com.aliyun.odps.table.DataSchema;
import com.aliyun.odps.table.SessionStatus;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.write.TableWriteSessionBuilder;
import com.aliyun.odps.task.MergeTask;
import com.google.gson.GsonBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.odps.output.writer.OdpsWriteOptions;
import org.apache.flink.odps.sink.common.AbstractWriteOperatorCoordinator;
import org.apache.flink.odps.sink.event.TaskAckEvent;
import org.apache.flink.odps.sink.event.SinkTaskEvent;
import org.apache.flink.odps.sink.table.TableUpsertSessionImpl;
import org.apache.flink.odps.sink.table.TableUtils;
import org.apache.flink.odps.sink.utils.NonThrownExecutor;
import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.odps.util.OdpsConf;
import org.apache.flink.odps.util.OdpsMetaDataProvider;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class UpsertOperatorCoordinator extends AbstractWriteOperatorCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(UpsertOperatorCoordinator.class);

    private final OdpsConf odpsConf;
    private final String projectName;
    private final String tableName;
    private final boolean isDynamicPartition;
    private final boolean supportsGrouping;
    private final OdpsWriteOptions writeOptions;
    private final DataSchema dataSchema;

    private final boolean isPartitioned;
    private String staticPartition;

    private transient Odps odps;
    private transient EnvironmentSettings settings;
    private transient OdpsMetaDataProvider tableMetaProvider;
    private transient Map<String, TableUpsertSessionImpl> tableUpsertSessionMap;
    private transient Map<String, Long> upsertCommitTimes;
    private transient NonThrownExecutor commitExecutor;

    public UpsertOperatorCoordinator(
            String operatorName,
            Configuration config,
            OperatorCoordinator.Context context,
            OdpsConf odpsConf,
            String projectName,
            String tableName,
            String partition,
            DataSchema dataSchema,
            boolean isDynamicPartition,
            boolean supportsGrouping,
            OdpsWriteOptions writeOptions) {
        super(operatorName, config, context);
        if (odpsConf == null) {
            this.odpsConf = OdpsUtils.getOdpsConf();
        } else {
            this.odpsConf = odpsConf;
        }
        Preconditions.checkNotNull(this.odpsConf, "odps conf cannot be null");
        if (this.odpsConf.isClusterMode()) {
            throw new IllegalStateException("Odps sink function cannot support overwrite in cluster mode.");
        }
        this.projectName = Preconditions.checkNotNull(projectName, "project cannot be null");
        this.tableName = Preconditions.checkNotNull(tableName, "table cannot be null");
        this.dataSchema = dataSchema;
        this.isDynamicPartition = isDynamicPartition;
        this.supportsGrouping = supportsGrouping;
        this.writeOptions = writeOptions == null ?
                OdpsWriteOptions.builder().build() : writeOptions;
        this.isPartitioned = !dataSchema.getPartitionKeys().isEmpty();
        this.tableMetaProvider = getTableMetaProvider();
        try {
            if (!isDynamicPartition) {
                checkPartition(partition);
            }
        } catch (IOException e) {
            throw new FlinkOdpsException(e);
        }
    }

    @Override
    protected void initEnv() {
        this.settings = TableUtils.getEnvironmentSettings(getOdps(), odpsConf.getTunnelEndpoint());
        this.tableUpsertSessionMap = new ConcurrentHashMap<>();
        this.upsertCommitTimes = new ConcurrentHashMap<>();
        this.commitExecutor = NonThrownExecutor.builder(LOG)
                .exceptionHook((errMsg, t) -> this.context.failJob(new FlinkOdpsException(errMsg, t)))
                // TODO: config
                .threadNum(4)
                .waitForTasksFinish(true).build();
    }

    @Override
    public void close() throws Exception {
        if (commitExecutor != null) {
            commitExecutor.close();
        }
        super.close();
    }

    @Override
    protected void handleBootstrapEvent(SinkTaskEvent event) {
        if (isDynamicPartition) {
            if (StringUtils.isNullOrWhitespaceOnly(event.getPartitionSpec())) {
                throw new FlinkOdpsException("Invalid partition spec for dynamic partition!");
            }
            try {
                initDynamicWriteSession(event.getTaskID(), event.getPartitionSpec());
            } catch (Exception e) {
                throw new FlinkOdpsException(e);
            }
        } else {
            this.eventBuffer[event.getTaskID()] = event;
            if (Arrays.stream(eventBuffer).allMatch(evt -> evt != null && evt.isBootstrap())) {
                try {
                    initStaticWriteSession();
                    this.eventBuffer = new SinkTaskEvent[this.parallelism];
                } catch (Exception e) {
                    throw new FlinkOdpsException(e);
                }
            }
        }
    }

    @Override
    protected void handleCommitEvent(SinkTaskEvent event) {
        this.eventBuffer[event.getTaskID()] = event;
        if (Arrays.stream(eventBuffer).allMatch(Objects::nonNull)) {
            try {
                commitWriteSession();
                if (!isDynamicPartition) {
                    initStaticWriteSession();
                } else {
                    executor.execute(() -> {
                        sendAllTaskAckEvents(-1, "", "", true);
                    }, "Commit session response");
                }
                this.eventBuffer = new SinkTaskEvent[this.parallelism];
            } catch (Exception e) {
                throw new FlinkOdpsException(e);
            }
        }
    }

    @Override
    protected void handleEndInputEvent(SinkTaskEvent event) {
        this.eventBuffer[event.getTaskID()] = event;
        if (Arrays.stream(eventBuffer).allMatch(evt -> evt != null && evt.isEndInput())) {
            try {
                commitWriteSession();
                this.eventBuffer = new SinkTaskEvent[this.parallelism];
            } catch (Exception e) {
                throw new FlinkOdpsException(e);
            }
        }
    }

    protected void initStaticWriteSession() throws IOException {
        executor.execute(() -> {
            TableUpsertSessionImpl upsertSession = doCreateWriteSession(staticPartition);
            sendAllTaskAckEvents(-1, staticPartition, upsertSession.getId(), true);
            tableUpsertSessionMap.put(staticPartition, upsertSession);
        }, "initialize session");
    }

    protected void initDynamicWriteSession(int taskId, String partitionSpec) throws IOException {
        executor.execute(() -> {
            String sessionId;
            if (tableUpsertSessionMap.containsKey(partitionSpec)) {
                sessionId = tableUpsertSessionMap.get(partitionSpec).getId();
            } else {
                createPartitionIfNeeded(partitionSpec);
                TableUpsertSessionImpl upsertSession = doCreateWriteSession(partitionSpec);
                sessionId = upsertSession.getId();
                tableUpsertSessionMap.put(partitionSpec, upsertSession);
            }
            sendSingleTaskAckEvents(-1, taskId, partitionSpec, sessionId, false);
        }, "initialize dynamic partition session");
    }

    private TableUpsertSessionImpl doCreateWriteSession(String partitionSpec) throws IOException {
        TableWriteSessionBuilder builder = new TableWriteSessionBuilder()
                .identifier(TableIdentifier.of(projectName, tableName))
                .withSessionProvider("upsert")
                .withSettings(settings);
        if (isPartitioned) {
            builder.partition(new PartitionSpec(partitionSpec));
        }
        TableUpsertSessionImpl upsertSession =
                (TableUpsertSessionImpl) builder.buildUpsertSession();
        LOG.info("Create session: {}, {}", upsertSession.getId(), upsertSession.getStatus());
        if (!upsertSession.getStatus().equals(SessionStatus.NORMAL)) {
            throw new IOException("Invalid session status: " + upsertSession.getStatus());
        }
        return upsertSession;
    }

    protected boolean commitWriteSession() throws IOException {
        if (Arrays.stream(eventBuffer).allMatch(Objects::nonNull)) {
            Set<String> partitionResults = Arrays.stream(eventBuffer)
                    .map(SinkTaskEvent::getWriterStatuses)
                    .flatMap(Collection::stream)
                    .map(WriterStatus::getPartitionSpec)
                    .collect(Collectors.toSet());
            if (!tableUpsertSessionMap.keySet().equals(partitionResults)) {
                throw new IllegalArgumentException("Expected partition " + tableUpsertSessionMap.keySet()
                        + ", actual partition " + partitionResults);
            }
            final CountDownLatch latch = new CountDownLatch(partitionResults.size());
            long startCommitTime = System.nanoTime();
            final Map<String, Boolean> commitStatus = new ConcurrentHashMap<>();
            for (String partition : partitionResults) {
                commitExecutor.execute(() -> {
                    TableUpsertSessionImpl session = tableUpsertSessionMap.get(partition);
                    try {
                        LOG.info("Start to commit session {}, partition {}", session.getId(), partition);
                        long startTime = System.nanoTime();
                        session.commit();

                        LOG.info("Commit session {}, time taken ms: {}", session.getId(),
                                NANOSECONDS.toMillis(System.nanoTime() - startTime));
                        commitStatus.put(partition, true);
                    } catch (Exception e) {
                        commitStatus.put(partition, false);
                        throw new IOException(e);
                    } finally {
                        session.close();
                        latch.countDown();
                    }
                    long commitTimes = upsertCommitTimes.getOrDefault(partition, 0L);
                    upsertCommitTimes.put(partition, ++commitTimes);
                }, "Commit upsert session for partition " + partition);
            }
            try {
                latch.await();
                // Clear session info
                tableUpsertSessionMap.clear();
                if (!commitStatus.values().stream().allMatch(status -> status == true)) {
                    throw new IOException("Commit error");
                }
                LOG.info("Checkpoint commit table partition size {}, time taken ms: {}", partitionResults.size(),
                        NANOSECONDS.toMillis(System.nanoTime() - startCommitTime));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            compactPartitions();
            return true;
        }
        return false;
    }

    private Odps getOdps() {
        if (odps == null) {
            this.odps = OdpsUtils.getOdps(this.odpsConf);
        }
        return odps;
    }

    private OdpsMetaDataProvider getTableMetaProvider() {
        if (tableMetaProvider == null) {
            tableMetaProvider = new OdpsMetaDataProvider(this.getOdps());
        }
        return tableMetaProvider;
    }

    private void checkPartition(String partitionSpec) throws IOException {
        if (isPartitioned) {
            if (StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                LOG.error("The partition cannot be null or whitespace with partition table: " + tableName);
                throw new IOException("Check partition failed.");
            } else {
                this.staticPartition = new PartitionSpec(partitionSpec).toString();
                createPartitionIfNeeded(this.staticPartition);
            }
        } else {
            if (!StringUtils.isNullOrWhitespaceOnly(partitionSpec)) {
                throw new IOException(
                        "The partition spec should be null or whitespace with non partition odps table: " + tableName);
            } else {
                this.staticPartition = "";
            }
        }
    }

    private void createPartitionIfNeeded(String targetPartition) throws IOException {
        int attemptNum = 1;
        while (true) {
            try {
                synchronized (this) {
                    Partition partition = getTableMetaProvider().getPartition(projectName,
                            tableName, targetPartition, true);
                    if (partition == null) {
                        Table table = getTableMetaProvider().getTable(projectName, tableName);
                        table.createPartition(new PartitionSpec(targetPartition), true);
                        LOG.info("Create partition: " + tableName + "/" + targetPartition);
                    }
                }
                break;
            } catch (Throwable e) {
                if (attemptNum++ > 5) {
                    LOG.error(
                            "Failed to create partition: " + targetPartition + " after retrying...");
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOG.error("Failed to create partition: " + targetPartition);
                    throw new IOException(e);
                }
            }
        }
    }

    private void sendAllTaskAckEvents(long checkpointId,
                                      String partition,
                                      String sessionId,
                                      boolean committed) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .committed(committed)
                .partition(partition)
                .sessionId(sessionId).build();
        CompletableFuture<?>[] futures = Arrays.stream(this.gateways).filter(Objects::nonNull)
                .map(gw -> gw.sendEvent(taskAckEvent))
                .toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).whenComplete((resp, error) -> {
            if (!sendToFinishedTasks(error)) {
                throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
            }
        });
    }

    private void sendSingleTaskAckEvents(long checkpointId,
                                         int taskId,
                                         String partition,
                                         String sessionId,
                                         boolean committed) {
        final TaskAckEvent taskAckEvent = TaskAckEvent.builder()
                .checkpointId(checkpointId)
                .committed(committed)
                .partition(partition)
                .sessionId(sessionId).build();
        if (gateways[taskId] == null) {
            throw new FlinkOdpsException("Gateway is null: " + taskId);
        } else {
            gateways[taskId].sendEvent(taskAckEvent).whenComplete((resp, error) -> {
                if (!sendToFinishedTasks(error)) {
                    throw new FlinkOdpsException("Error while waiting for the commit ack events to finish sending", error);
                }
            });
        }
    }

    private static boolean sendToFinishedTasks(Throwable throwable) {
        return throwable.getCause() instanceof TaskNotRunningException
                || throwable.getCause().getMessage().contains("running");
    }

    private void majorCompact(String partitionName) throws IOException {
        // TODO: project name?
        String tableInfo = tableName;
        if (isPartitioned) {
            PartitionSpec part = new PartitionSpec(partitionName);
            tableInfo += " partition(";
            tableInfo += part.toString();
            tableInfo += ")";
        }
        MergeTask task = new MergeTask("merge_task_for_" + tableName, tableInfo);
        Job job = new Job();
        String guid = UUID.randomUUID().toString();
        task.setProperty("guid", guid);
        Map<String, String> hints = new HashMap<>();
        hints.put("odps.merge.task.mode", "service");
        hints.put("odps.merge.quickmerge.flag", "false");
        hints.put("odps.merge.restructure.action", "hardlink");
        hints.put("odps.merge.txn.table.compact", "major_compact");
        task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(hints));
        job.addTask(task);
        try {
            Instance instance = odps.instances().create(job);
            long startTime = System.nanoTime();
            instance.waitForSuccess();
            LOG.info("Major compact table {}, partition {}, time taken ms: {}",
                    tableName, partitionName, NANOSECONDS.toMillis(System.nanoTime() - startTime));
        } catch (OdpsException e) {
            throw new IOException(e);
        }
    }

    /**
     * Checks the buffer is ready to commit.
     */
    private boolean allEventsReceived() {
        return Arrays.stream(eventBuffer)
                .allMatch(event -> event != null);
    }

    /**
     * Provider for {@link UpsertOperatorCoordinator}.
     */
    public static class Provider implements OperatorCoordinator.Provider {
        private final String operatorName;
        private final OperatorID operatorId;
        private final UpsertOperatorFactory.OdpsUpsertOperatorFactoryBuilder builder;

        public Provider(String operatorName,
                        OperatorID operatorId,
                        UpsertOperatorFactory.OdpsUpsertOperatorFactoryBuilder builder) {
            this.operatorName = operatorName;
            this.operatorId = operatorId;
            this.builder = builder;
        }

        @Override
        public OperatorID getOperatorId() {
            return this.operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new UpsertOperatorCoordinator(
                    operatorName,
                    builder.getConf(),
                    context,
                    builder.getOdpsConf(),
                    builder.getProjectName(),
                    builder.getTableName(),
                    builder.getPartition(),
                    builder.getDataSchema(),
                    builder.isDynamicPartition(),
                    builder.isSupportPartitionGrouping(),
                    builder.getWriteOptions());
        }
    }

    private void compactPartitions() throws IOException {
        List<String> compactPartitions = new LinkedList<>();
        for (Map.Entry<String, Long> entry : upsertCommitTimes.entrySet()) {
            if (entry.getValue() >= 1) {
                compactPartitions.add(entry.getKey());
            }
        }
        if (compactPartitions.size() > 0) {
            final CountDownLatch compactLatch = new CountDownLatch(compactPartitions.size());
            long startCompactTime = System.nanoTime();

            for (String partition : compactPartitions) {
                commitExecutor.execute(() -> {
                    try {
                        majorCompact(partition);
                        upsertCommitTimes.put(partition, 0L);
                    } catch (Exception e) {
                        throw new IOException(e);
                    } finally {
                        compactLatch.countDown();
                    }
                }, "Compact upsert session");
            }
            try {
                compactLatch.await();
                LOG.info("Checkpoint compact table partition size {}, time taken ms: {}", compactPartitions.size(),
                        NANOSECONDS.toMillis(System.nanoTime() - startCompactTime));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }
}