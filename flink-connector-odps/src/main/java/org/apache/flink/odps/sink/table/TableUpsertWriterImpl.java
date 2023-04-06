/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.table;

import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.table.metrics.Metrics;
import com.aliyun.odps.table.metrics.count.BytesCount;
import com.aliyun.odps.table.metrics.count.RecordCount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.streams.UpsertStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TableUpsertWriterImpl implements UpsertWriter<ArrayRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(TableUpsertWriterImpl.class);

    protected TableTunnel.UpsertSession session;
    protected UpsertStream stream;
    private boolean isClosed;
    private Metrics metrics;
    private BytesCount bytesCount;
    private RecordCount recordCount;

    public TableUpsertWriterImpl(TableTunnel.UpsertSession session) throws IOException {
        this.session = session;
        this.isClosed = false;
        initMetrics();

        UpsertStream.Listener listener = new UpsertStream.Listener() {
            @Override
            public void onFlush(UpsertStream.FlushResult result) {
                recordCount.inc(result.recordCount);
                bytesCount.inc(result.flushSize);
                LOG.info("Flush success, trace id: {}, time: {}, record: {}",
                        result.traceId, result.flushTime, result.recordCount);
            }
            @Override
            public boolean onFlushFail(String error, int retry) {
                LOG.error("Flush failed error:" + error);
                if (retry > 3) {
                    return false;
                }
                LOG.warn(String.format("Start to retry, retryCount: %d", retry));
                return true;
            }
        };
        try {
            this.stream = session.buildUpsertStream().setListener(listener).build();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    private void initMetrics() {
        this.bytesCount = new BytesCount();
        this.recordCount = new RecordCount();
        this.metrics = new Metrics();
        this.metrics.register(bytesCount);
        this.metrics.register(recordCount);
    }

    @Override
    public ArrayRecord newElement() {
        return (ArrayRecord) session.newRecord();
    }

    @Override
    public void upsert(ArrayRecord record) throws IOException {
        try {
            this.stream.upsert(record);
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void delete(ArrayRecord record) throws IOException {
        try {
            this.stream.delete(record);
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            this.stream.flush();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        try {
            this.stream.close();
        } catch (TunnelException e) {
            throw new IOException(e);
        }
        isClosed = true;
    }

    @Override
    public Metrics currentMetricsValues() {
        return this.metrics;
    }
}
