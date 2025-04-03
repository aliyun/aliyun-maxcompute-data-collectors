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

import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MaxComputeInputSplit
{
    private String sessionId;
    private Integer splitIndex;
    private Long startIndex;
    private Long numRecord;

    @JsonCreator
    public MaxComputeInputSplit(
            @JsonProperty("sessionId") String sessionId, 
            @JsonProperty("splitIndex") Integer splitIndex,
            @JsonProperty("startIndex") Long startIndex, @JsonProperty("numRecord") Long numRecord)
    {
        this.sessionId = requireNonNull(sessionId, "schema name is null");
        this.splitIndex = requireNonNull(splitIndex, "connector id is null");
        this.startIndex = requireNonNull(startIndex, "table name is null");
        this.numRecord = requireNonNull(numRecord, "inputSplit is null");
    }

    public MaxComputeInputSplit(InputSplit inputSplit)
    {
        sessionId = requireNonNull(inputSplit, "input split is null").getSessionId();
        if (inputSplit instanceof IndexedInputSplit) {
            splitIndex = ((IndexedInputSplit) inputSplit).getSplitIndex();
            startIndex = -1L;
            numRecord = -1L;
        }
        else if (inputSplit instanceof RowRangeInputSplit) {
            startIndex = ((RowRangeInputSplit) inputSplit).getRowRange().getStartIndex();
            numRecord = ((RowRangeInputSplit) inputSplit).getRowRange().getNumRecord();
            splitIndex = -1;
        }
    }

    public InputSplit toInputSplit()
    {
        if (splitIndex != null && splitIndex >= 0) {
            return new IndexedInputSplit(sessionId, splitIndex);
        }
        else if (startIndex != null && numRecord != null) {
            return new RowRangeInputSplit(sessionId, startIndex, numRecord);
        }
        else {
            throw new IllegalArgumentException("invalid input split");
        }
    }

    @JsonProperty
    public String getSessionId()
    {
        return sessionId;
    }

    @JsonProperty
    public Integer getSplitIndex()
    {
        return splitIndex;
    }

    @JsonProperty
    public Long getStartIndex()
    {
        return startIndex;
    }

    @JsonProperty
    public Long getNumRecord()
    {
        return numRecord;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sessionId", sessionId)
                .add("splitIndex", splitIndex)
                .toString();
    }
}
