package org.apache.flink.odps.sink.event;

import org.apache.flink.odps.sink.utils.WriterStatus;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SinkTaskEvent implements OperatorEvent {

    private final Integer taskID;
    private final Long checkpointID;

    private List<WriterStatus> writerStatuses;
    private String partitionSpec;
    private boolean endInput;
    private boolean bootstrap;

    private SinkTaskEvent(
            int taskID,
            Long checkpointID,
            List<WriterStatus> writerStatuses,
            String partitionSpec,
            boolean bootstrap,
            boolean endInput) {
        this.taskID = taskID;
        this.checkpointID = checkpointID;
        this.writerStatuses = new ArrayList<>(writerStatuses);
        this.partitionSpec = partitionSpec;
        this.bootstrap = bootstrap;
        this.endInput = endInput;
    }

    public Integer getTaskID() {
        return taskID;
    }

    public List<WriterStatus> getWriterStatuses() {
        return writerStatuses;
    }

    public boolean isBootstrap() {
        return bootstrap;
    }

    public boolean isEndInput() {
        return endInput;
    }

    public Long getCheckpointID() {
        return checkpointID;
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link SinkTaskEvent}.
     */
    public static class Builder {
        private Integer taskID;
        private List<WriterStatus> writerStatuses = new ArrayList<>();
        private boolean endInput = false;
        private boolean bootstrap = false;
        private String partitionSpec = "";
        private Long checkpointID = -1L;

        public SinkTaskEvent build() {
            Objects.requireNonNull(taskID);
            Objects.requireNonNull(writerStatuses);
            return new SinkTaskEvent(taskID, checkpointID, writerStatuses, partitionSpec, bootstrap, endInput);
        }

        public Builder taskID(int taskID) {
            this.taskID = taskID;
            return this;
        }

        public Builder checkpointID(long checkpointID) {
            this.checkpointID = checkpointID;
            return this;
        }

        public Builder writeStatus(List<WriterStatus> writerStatuses) {
            this.writerStatuses = writerStatuses;
            return this;
        }

        public Builder requiredPartition(String partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder endInput(boolean endInput) {
            this.endInput = endInput;
            return this;
        }

        public Builder bootstrap(boolean bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }
    }
}
