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

import java.io.Serializable;
import java.util.Objects;

/** Options for the JDBC lookup. */
public class OdpsLookupOptions implements Serializable {

    private final long cacheExpireMs;
    private final int maxRetryTimes;

    public OdpsLookupOptions(long cacheExpireMs, int maxRetryTimes) {
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof OdpsLookupOptions) {
            OdpsLookupOptions options = (OdpsLookupOptions) o;
            return Objects.equals(cacheExpireMs, options.cacheExpireMs)
                    && Objects.equals(maxRetryTimes, options.maxRetryTimes);
        } else {
            return false;
        }
    }

    /** Builder of {@link OdpsLookupOptions}. */
    public static class Builder {
        private long cacheExpireMs = 300 * 1000L;
        private int maxRetryTimes = 3;

        /** optional, lookup cache expire mills, over this time, the old data will expire. */
        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        /** optional, max retry times for jdbc connector. */
        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public OdpsLookupOptions build() {
            return new OdpsLookupOptions(cacheExpireMs, maxRetryTimes);
        }
    }
}
