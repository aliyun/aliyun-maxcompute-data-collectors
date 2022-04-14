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

package org.apache.flink.odps.util;

import com.aliyun.odps.PartitionSpec;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

public class PartitionPath implements Serializable {
    private final String projectName;
    private final String tableName;
    private final String partitionSpec;

    public PartitionPath(String projectName, String tableName, String partitionSpec) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(projectName), "projectName cannot be null or empty");
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(tableName), "tableName cannot be null or empty");
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(partitionSpec), "partitionSpec cannot be null or empty");
        this.projectName = projectName;
        this.tableName = tableName;
        this.partitionSpec = new PartitionSpec(partitionSpec).toString();
    }

    public String getPartitionSpec() {
        return partitionSpec;
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFullName() {
        return String.format("%s.%s.%s", this.projectName, this.tableName, this.partitionSpec);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            PartitionPath that = (PartitionPath) o;
            return Objects.equals(this.projectName, that.projectName)
                    && Objects.equals(this.tableName, that.tableName)
                    && Objects.equals(this.partitionSpec, that.partitionSpec);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.projectName, this.tableName, this.partitionSpec);
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", this.projectName, this.tableName, this.partitionSpec);
    }
}
