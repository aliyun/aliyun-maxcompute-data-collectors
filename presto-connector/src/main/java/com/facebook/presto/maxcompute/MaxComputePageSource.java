/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.maxcompute;

import com.aliyun.odps.Odps;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.table.configuration.CompressionCodec;
import com.aliyun.odps.table.configuration.ReaderOptions;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.SplitReader;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.maxcompute.utils.ArrowToPageConverter;
import com.facebook.presto.maxcompute.utils.ArrowUtils;
import com.facebook.presto.maxcompute.utils.MaxComputeUtils;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MaxComputePageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(MaxComputePageSource.class);

    private final AtomicLong readRowCount = new AtomicLong();
    private final ArrowToPageConverter arrowToPageConverter;
    private final BufferAllocator allocator;
    private final PageBuilder pageBuilder;
    private final SplitReader<VectorSchemaRoot> reader;
    private final Odps odps;
    private final MaxComputeTableHandle tableHandle;
    private boolean isFinished;

    public MaxComputePageSource(
            MaxComputeConfig config,
            MaxComputeTableLayoutHandle tableLayoutHandle,
            List<ColumnHandle> requireColumns,
            MaxComputeSplit split)
    {
        this.odps = MaxComputeUtils.getOdps(requireNonNull(config, "connector config is null"));
        this.tableHandle = requireNonNull(tableLayoutHandle, "tableLayoutHandle is null").getTableHandle();
        requireNonNull(split, "split is null");
        this.isFinished = false;

        LOG.info(String.format("create maxcompute page source, requireColumns: %s, split: %s", requireColumns, split));
        TableSchema schema = odps.tables().get(tableHandle.getProjectId(), tableHandle.getSchemaName(), tableHandle.getTableName()).getSchema();
        this.allocator = ArrowUtils.getRootAllocator().newChildAllocator(UUID.randomUUID().toString(), 1024, Long.MAX_VALUE);
        this.arrowToPageConverter = new ArrowToPageConverter(requireColumns, schema.getAllColumns());
        this.pageBuilder = new PageBuilder(requireColumns.stream()
                .map(col -> ((MaxComputeColumnHandle) col).getType())
                .collect(toImmutableList()));
        EnvironmentSettings environmentSettings = MaxComputeUtils.getEnvironmentSettings(config);
        try {
            this.reader = split.getReadSession().createArrowReader(split.getSplit().toInputSplit(),
                    ReaderOptions.newBuilder().withMaxBatchRowCount(4096)
                            .withCompressionCodec(CompressionCodec.ZSTD)
                            .withSettings(environmentSettings).build());
        }
        catch (IOException e) {
            throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "create arrow reader error", e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedPositions()
    {
        return readRowCount.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        try {
            return isFinished;
        }
        catch (Exception e) {
            throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "check page is finished error", e);
        }
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        try {
            if (!reader.hasNext()) {
                isFinished = true;
                return null;
            }
        }
        catch (IOException e) {
            throw new PrestoException(MaxComputeErrorCode.MAXCOMPUTE_CONNECTOR_ERROR, "check page is finished error", e);
        }
        VectorSchemaRoot vectorSchemaRoot = reader.get();
        readRowCount.addAndGet(vectorSchemaRoot.getRowCount());
        arrowToPageConverter.convert(pageBuilder, vectorSchemaRoot);
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return allocator.getAllocatedMemory() + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        LOG.info(String.format("success read %s rows", readRowCount.get()));
        try {
            reader.close();
        }
        catch (IOException e) {
            LOG.error(e, "close reader error");
        }
        allocator.close();
    }
}
