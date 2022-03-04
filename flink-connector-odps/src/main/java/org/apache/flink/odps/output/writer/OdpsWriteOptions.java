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

package org.apache.flink.odps.output.writer;
import java.io.Serializable;
import java.util.Objects;

/** Options for Odps writing. */
public class OdpsWriteOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxRows;
    private final long bufferFlushIntervalMillis;
    private final int writeMaxRetries;
    private final int dynamicPartitionLimit;
    private final String dynamicPartitionDefaultValue;
    private final String dynamicPartitionAssignerClass;

    public OdpsWriteOptions(
            long bufferFlushMaxSizeInBytes,
            long bufferFlushMaxMutations,
            long bufferFlushIntervalMillis,
            int writeMaxRetries,
            int dynamicPartitionLimit,
            String dynamicPartitionDefaultValue,
            String dynamicPartitionAssignerClass) {
        this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
        this.bufferFlushMaxRows = bufferFlushMaxMutations;
        this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
        this.writeMaxRetries = writeMaxRetries;
        this.dynamicPartitionLimit = dynamicPartitionLimit;
        this.dynamicPartitionDefaultValue = dynamicPartitionDefaultValue;
        this.dynamicPartitionAssignerClass = dynamicPartitionAssignerClass;
    }

    public long getBufferFlushMaxSizeInBytes() {
        return bufferFlushMaxSizeInBytes;
    }

    public long getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public int getWriteMaxRetries() {
        return writeMaxRetries;
    }

    public int getDynamicPartitionLimit() {
        return dynamicPartitionLimit;
    }

    public String getDynamicPartitionDefaultValue() {
        return dynamicPartitionDefaultValue;
    }

    public String getDynamicPartitionAssignerClass() {
        return dynamicPartitionAssignerClass;
    }

    @Override
    public String toString() {
        return "OdpsWriteOptions{"
                + "bufferFlushMaxSizeInBytes="
                + bufferFlushMaxSizeInBytes
                + ", bufferFlushMaxRows="
                + bufferFlushMaxRows
                + ", bufferFlushIntervalMillis="
                + bufferFlushIntervalMillis
                + ", writeMaxRetries="
                + writeMaxRetries
                + ", dynamicPartitionLimit="
                + dynamicPartitionLimit
                + ", dynamicPartitionDefaultValue="
                + dynamicPartitionDefaultValue
                + ", dynamicPartitionAssignerClass="
                + dynamicPartitionAssignerClass
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OdpsWriteOptions that = (OdpsWriteOptions) o;
        return bufferFlushMaxSizeInBytes == that.bufferFlushMaxSizeInBytes
                && bufferFlushMaxRows == that.bufferFlushMaxRows
                && bufferFlushIntervalMillis == that.bufferFlushIntervalMillis
                && writeMaxRetries == that.writeMaxRetries
                && dynamicPartitionLimit == that.dynamicPartitionLimit
                && Objects.equals(dynamicPartitionDefaultValue, that.dynamicPartitionDefaultValue)
                && Objects.equals(dynamicPartitionAssignerClass, that.dynamicPartitionAssignerClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bufferFlushMaxSizeInBytes,
                bufferFlushMaxRows,
                bufferFlushIntervalMillis,
                writeMaxRetries,
                dynamicPartitionLimit,
                dynamicPartitionDefaultValue,
                dynamicPartitionAssignerClass);
    }

    /** Creates a builder for {@link OdpsWriteOptions}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for {@link OdpsWriteOptions}. */
    public static class Builder {

        private long bufferFlushMaxSizeInBytes = 4 * 1024L * 1024L;
        private long bufferFlushMaxRows = 1000L;
        private long bufferFlushIntervalMillis = 300 * 1000L;
        private int writeMaxRetries = 3;
        private int dynamicPartitionLimit = 20;
        private String dynamicPartitionDefaultValue;
        private String dynamicPartitionAssignerClass;

        public Builder setBufferFlushMaxSizeInBytes(long bufferFlushMaxSizeInBytes) {
            this.bufferFlushMaxSizeInBytes = bufferFlushMaxSizeInBytes;
            return this;
        }

        public Builder setBufferFlushMaxRows(long bufferFlushMaxRows) {
            this.bufferFlushMaxRows = bufferFlushMaxRows;
            return this;
        }

        public Builder setBufferFlushIntervalMillis(long bufferFlushIntervalMillis) {
            this.bufferFlushIntervalMillis = bufferFlushIntervalMillis;
            return this;
        }

        public Builder setWriteMaxRetries(int writeMaxRetries) {
            this.writeMaxRetries = writeMaxRetries;
            return this;
        }

        public Builder setDynamicPartitionLimit(int dynamicPartitionLimit) {
            this.dynamicPartitionLimit = dynamicPartitionLimit;
            return this;
        }

        public Builder setDynamicPartitionDefaultValue(String dynamicPartitionDefaultValue) {
            this.dynamicPartitionDefaultValue = dynamicPartitionDefaultValue;
            return this;
        }

        public Builder setDynamicPartitionAssignerClass(String dynamicPartitionAssignerClass) {
            this.dynamicPartitionAssignerClass = dynamicPartitionAssignerClass;
            return this;
        }

        /** Creates a new instance of {@link OdpsWriteOptions}. */
        public OdpsWriteOptions build() {
            return new OdpsWriteOptions(
                    bufferFlushMaxSizeInBytes,
                    bufferFlushMaxRows,
                    bufferFlushIntervalMillis,
                    writeMaxRetries,
                    dynamicPartitionLimit,
                    dynamicPartitionDefaultValue,
                    dynamicPartitionAssignerClass);
        }
    }
}
