package org.apache.flink.odps.sink.utils;

import com.aliyun.odps.data.ArrayRecord;

public class DataItem {

    private final String partition; // record partition
    private final ArrayRecord data; // record payload
    private final RecordOperationType operation; // operation

    public DataItem(String partition, ArrayRecord data, RecordOperationType operation) {
        this.partition = partition;
        this.data = data;
        this.operation = operation;
    }

    public String getPartition() {
        return partition;
    }

    public ArrayRecord getData() {
        return data;
    }

    public RecordOperationType getOperation() {
        return operation;
    }
}