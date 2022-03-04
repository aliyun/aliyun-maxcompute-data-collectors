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

package org.apache.flink.odps.input;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsLookupFunction extends TableFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(OdpsLookupFunction.class);

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

    private final OdpsInputFormat<RowData> inputFormat;

    // cache for lookup data
    private transient Map<RowData, List<RowData>> cache;
    private transient TypeSerializer<RowData> serializer;

    private final RowType rowType;
    private final int[] lookupCols;

    private final RowData.FieldGetter[] lookupFieldGetters;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private transient long nextLoadTime;

    public OdpsLookupFunction(
            OdpsInputFormat<RowData> inputFormat,
            OdpsLookupOptions options,
            int[] lookupKeys,
            RowType rowType) {
        this.inputFormat = inputFormat;
        this.maxRetryTimes = options.getMaxRetryTimes();
        this.cacheExpireMs = options.getCacheExpireMs();
        this.lookupCols = lookupKeys;
        this.rowType = rowType;
        this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
        for (int i = 0; i < lookupKeys.length; i++) {
            lookupFieldGetters[i] =
                    RowData.createFieldGetter(rowType.getTypeAt(lookupKeys[i]), lookupKeys[i]);
        }
    }

    @Override
    public TypeInformation<RowData> getResultType() {
        return InternalTypeInfo.of(rowType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        cache = new HashMap<>();
        nextLoadTime = -1;
        serializer = getResultType().createSerializer(new ExecutionConfig());
    }

    public void eval(Object... values) {
        Preconditions.checkArgument(values.length == lookupCols.length,
                "Number of values and lookup keys mismatch");
        checkCacheReload();
        RowData lookupKey = GenericRowData.of(values);
        List<RowData> matchedRows = cache.get(lookupKey);
        if (matchedRows != null) {
            for (RowData matchedRow : matchedRows) {
                collect(matchedRow);
            }
        }
    }

    private void checkCacheReload() {
        if (nextLoadTime > System.currentTimeMillis()) {
            return;
        }
        if (nextLoadTime > 0) {
            LOG.info("Lookup join cache has expired, reloading");
        } else {
            LOG.info("Populating lookup join cache");
        }
        int numRetry = 0;
        while (true) {
            cache.clear();
            try {
                OdpsInputSplit[] inputSplits = inputFormat.createInputSplits(1);
                long count = 0;
                GenericRowData reuse = new GenericRowData(rowType.getFieldCount());
                RowData row;
                for (OdpsInputSplit split : inputSplits) {
                    inputFormat.open(split);
                    while (!inputFormat.reachedEnd()) {
                        row = inputFormat.nextRecord(reuse);
                        count++;
                        RowData key = extractLookupKey(row);
                        List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
                        rows.add(serializer.copy(row));
                    }
                    inputFormat.close();
                }
                nextLoadTime = System.currentTimeMillis() + cacheExpireMs;
                LOG.info("Loaded {} row(s) into lookup join cache", count);
                return;
            } catch (IOException e) {
                if (numRetry >= maxRetryTimes) {
                    throw new FlinkRuntimeException(
                            String.format("Failed to load table into cache after %d retries", numRetry), e);
                }
                numRetry++;
                long toSleep = numRetry * RETRY_INTERVAL.toMillis();
                LOG.warn(String.format("Failed to load table into cache, will retry in %d seconds", toSleep / 1000), e);
                try {
                    Thread.sleep(toSleep);
                } catch (InterruptedException ex) {
                    LOG.warn("Interrupted while waiting to retry failed cache load, aborting");
                    throw new FlinkRuntimeException(ex);
                }
            }
        }
    }

    private RowData extractLookupKey(RowData row) {
        GenericRowData key = new GenericRowData(lookupFieldGetters.length);
        for (int i = 0; i < lookupFieldGetters.length; i++) {
            key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
        }
        return key;
    }
}
