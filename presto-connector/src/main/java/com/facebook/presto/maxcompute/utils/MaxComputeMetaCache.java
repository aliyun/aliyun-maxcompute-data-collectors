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

package com.facebook.presto.maxcompute.utils;

import com.aliyun.odps.Odps;
import com.facebook.presto.maxcompute.MaxComputeConfig;
import com.facebook.presto.maxcompute.MaxComputeTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import static java.util.concurrent.TimeUnit.HOURS;

public class MaxComputeMetaCache
{
    private final Odps odps;
    private LoadingCache<MaxComputeTableHandle, ConnectorTableMetadata> tableMetadataCache;

    public MaxComputeMetaCache(MaxComputeConfig config)
    {
        this.odps = MaxComputeUtils.getOdps(config);

        // TODO: cache options
        tableMetadataCache = newCacheBuilder(6400, 10000).build(
                new CacheLoader<MaxComputeTableHandle, ConnectorTableMetadata>()
                {
                    @Override
                    public ConnectorTableMetadata load(MaxComputeTableHandle tableHandle)
                    {
                        return MaxComputeUtils.getTableMetadata(odps, tableHandle);
                    }
                });
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long maximumSize)
    {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, HOURS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key)
    {
        try {
            return cache.get(key);
        }
        catch (Exception e) {
            return null;
        }
    }

    private static <K, V> void write(LoadingCache<K, V> cache, K key, V value)
    {
        try {
            cache.put(key, value);
        }
        catch (Exception e) {
            // ignore
        }
    }

    public ConnectorTableMetadata getTableMetadata(MaxComputeTableHandle tableHandle)
    {
        return get(tableMetadataCache, tableHandle);
    }

    public void writeTableMetadata(MaxComputeTableHandle tableHandle, ConnectorTableMetadata tableMetadata)
    {
        write(tableMetadataCache, tableHandle, tableMetadata);
    }
}
