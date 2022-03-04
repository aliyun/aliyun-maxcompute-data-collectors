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

package org.apache.flink.odps.output.stream;

import com.aliyun.odps.PartitionSpec;
import org.apache.flink.odps.util.OdpsUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;

public class DateTimePartitionAssigner<T> implements PartitionAssigner<T> {

    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";
    public static final String DEFAULT_PARTITION_NAME = "dt";

    private final String partitionName;

    private final String formatString;

    private final ZoneId zoneId;

    private transient DateTimeFormatter dateTimeFormatter;

    public DateTimePartitionAssigner() {
        this(DEFAULT_FORMAT_STRING);
    }

    public DateTimePartitionAssigner(String formatString) {
        this(formatString, ZoneId.systemDefault());
    }

    public DateTimePartitionAssigner(String formatString, ZoneId zoneId) {
        this(DEFAULT_PARTITION_NAME, formatString, zoneId);
    }

    public DateTimePartitionAssigner(ZoneId zoneId) {
        this(DEFAULT_FORMAT_STRING, zoneId);
    }

    public DateTimePartitionAssigner(String partitionName, String formatString) {
        this(partitionName, formatString, ZoneId.systemDefault());
    }

    public DateTimePartitionAssigner(String partitionName, String formatString, ZoneId zoneId) {
        this.partitionName =  Preconditions.checkNotNull(partitionName);
        this.formatString = Preconditions.checkNotNull(formatString);
        this.zoneId = Preconditions.checkNotNull(zoneId);
    }

    @Override
    public String getPartitionSpec(T element, Context context) {
        if (dateTimeFormatter == null) {
            dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
        }
        String staticPartition = context.getStaticPartition();
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(staticPartition)) {
            PartitionSpec partitionSpec = new PartitionSpec(staticPartition);
            partitionSpec.keys().forEach((key)->{
                partSpec.put(key, partitionSpec.get(key));
            });
        }
        partSpec.put(partitionName,
                dateTimeFormatter.format(Instant.ofEpochMilli(context.currentProcessingTime())));
        return OdpsUtils.generatePartition(partSpec);
    }

    @Override
    public String toString() {
        return "DateTimeBucketAssigner{" +
                "partitionName='" + partitionName + '\'' +
                ", formatString='" + formatString + '\'' +
                ", zoneId=" + zoneId +
                '}';
    }
}
