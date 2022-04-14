/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.output.stream;

import org.apache.flink.annotation.Public;
import org.apache.flink.odps.output.writer.OdpsWriterInfo;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Public
public final class DefaultSinkPolicy<IN> implements SinkPolicy<IN> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(DefaultSinkPolicy.class);

	private final long blockCount;
	private final long commitInterval;
	private final boolean rollOnCheckpoint;

	/**
	 * Private constructor to avoid direct instantiation.
	 */
	private DefaultSinkPolicy(long blockCount, long commitInterval, boolean rollOnCheckpoint) {
		Preconditions.checkArgument(blockCount > 0L);
		Preconditions.checkArgument(blockCount < 20000L);
		Preconditions.checkArgument(commitInterval > 0L);
		Preconditions.checkState(commitInterval < 86400000L);
		this.blockCount = blockCount;
		this.commitInterval = commitInterval;
		this.rollOnCheckpoint = rollOnCheckpoint;
	}

	/**
	 * Creates a new {@link PolicyBuilder} that is used to configure and build
	 * an instance of {@code DefaultCommitPolicy}.
	 */
	public static PolicyBuilder builder(long maxBlockCount, long commitInterval, boolean rollOnCheckpoint) {
		return new PolicyBuilder(
				maxBlockCount,
				commitInterval,
				rollOnCheckpoint);
	}

	@Override
	public boolean shouldCommitOnCheckpoint(long checkPointId, long checkPointTimeStamp, Map<String, OdpsWriterInfo> odpsWriteInfoMap) {
		return false;
	}

	@Override
	public boolean shouldCommitOnEvent(IN element, Map<String, OdpsWriterInfo> odpsWriteInfoMap) {
		return false;
	}

	@Override
	public boolean shouldCommitOnProcessingTime(long currentTime, Map<String, OdpsWriterInfo> odpsWriteInfoMap) {
		if (odpsWriteInfoMap.isEmpty()) {
			LOG.info("should not commit write session due to odps write info is empty");
			return false;
		}
		for (Map.Entry<String, OdpsWriterInfo> entry : odpsWriteInfoMap.entrySet()) {
			if (currentTime - entry.getValue().getSessionCreateTime() > commitInterval) {
				LOG.info("commit write session due to create time over {} ms.", currentTime - entry.getValue().getSessionCreateTime());
				return true;
			}
			if (entry.getValue().getCurrentBlockId() > blockCount) {
				LOG.info("commit write session due to block count over {}.", entry.getValue().getCurrentBlockId());
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean shouldRollOnCheckpoint(long checkPointId, long checkPointTimeStamp, OdpsWriterInfo odpsWriterInfo) {
		return rollOnCheckpoint;
	}

	public static final class PolicyBuilder {

		private final long blockCount;
		private final long commitInterval;
		private final boolean rollOnCheckpoint;

		private PolicyBuilder(
				final long blockCount,
				final long commitInterval,
				final boolean rollOnCheckpoint) {
			this.blockCount = blockCount;
			this.commitInterval = commitInterval;
			this.rollOnCheckpoint = rollOnCheckpoint;
		}

		public PolicyBuilder withBlockCount(final long size) {
			Preconditions.checkState(size > 0L);
			Preconditions.checkArgument(blockCount < 20000L);
			return new PolicyBuilder(size, commitInterval, rollOnCheckpoint);
		}

		public PolicyBuilder withCommitInterval(final long interval) {
			Preconditions.checkState(interval > 0L);
			Preconditions.checkState(interval < 86400000L);
			return new PolicyBuilder(blockCount, interval, rollOnCheckpoint);
		}

		public PolicyBuilder withRollOnCheckpoint(final long interval) {
			Preconditions.checkState(interval > 0L);
			Preconditions.checkState(interval < 86400000L);
			return new PolicyBuilder(blockCount, interval, rollOnCheckpoint);
		}

		public <IN> DefaultSinkPolicy<IN> build() {
			return new DefaultSinkPolicy<>(blockCount, commitInterval, rollOnCheckpoint);
		}
	}
}
