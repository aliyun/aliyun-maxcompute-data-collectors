package org.apache.flink.odps.sink.utils;

import java.util.LinkedList;
import java.util.List;

public class DataBucket {
    private final List<DataItem> records;
    private final String partitionPath;

    public DataBucket(long batchSize, String partitionPath) {
        this.records = new LinkedList<>();
        this.partitionPath = partitionPath;
    }

    public void add(DataItem item) {
        this.records.add(item);
    }

    public boolean isEmpty() {
        return this.records.isEmpty();
    }

    public List<DataItem> getRecords() {
        return this.records;
    }

    public String getPartitionPath() {
        return partitionPath;
    }

    public void reset() {
        this.records.clear();
    }
}