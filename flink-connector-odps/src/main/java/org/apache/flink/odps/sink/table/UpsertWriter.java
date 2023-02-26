package org.apache.flink.odps.sink.table;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.table.metrics.Metrics;

import java.io.Closeable;
import java.io.IOException;

public interface UpsertWriter<T> extends Closeable {

    T newElement();

    /**
     * 按upsert操作，写入一条{@link Record}对象到缓冲区
     *
     * @param record - record对象
     */
    void upsert(T record) throws IOException;

    /**
     * 按delete操作，写入一条{@link Record}对象到缓冲区
     *
     * @param record - record对象
     */
    void delete(T record) throws IOException;

    /**
     * 发送缓冲区数据到服务端
     */
    void flush() throws IOException;

    /**
     * Returns metrics for this writer
     */
    default Metrics currentMetricsValues() {
        return new Metrics();
    }
}
