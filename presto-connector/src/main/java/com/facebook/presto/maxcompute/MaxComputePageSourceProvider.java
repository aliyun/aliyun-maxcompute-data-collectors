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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class MaxComputePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private MaxComputeConfig config;

    @Inject
    public MaxComputePageSourceProvider(MaxComputeConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, ConnectorTableLayoutHandle layout, List<ColumnHandle> columns, SplitContext splitContext)
    {
        return new MaxComputePageSource(config, (MaxComputeTableLayoutHandle) layout, columns, (MaxComputeSplit) split);
    }
}
