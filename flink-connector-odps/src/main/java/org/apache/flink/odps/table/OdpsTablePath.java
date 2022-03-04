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

package org.apache.flink.odps.table;

import org.apache.flink.util.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

public class OdpsTablePath {

    private final String projectName;
    private final String tableName;

    public OdpsTablePath(String projectName, String tableName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(projectName));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tableName));

        this.projectName = projectName;
        this.tableName = tableName;
    }

    public static OdpsTablePath fromTablePath(String tablePath) {
        if (tablePath.contains(".")) {
            String[] path = tablePath.split("\\.");

            checkArgument(
                    path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            tablePath, path.length));

            return new OdpsTablePath(path[0], path[1]);
        } else {
            throw new IllegalArgumentException(
                    String.format("Table name '%s' is not valid. Please set default project", tablePath));
        }
    }

    public static OdpsTablePath fromTablePath(String defaultProject, String tablePath) {
        if (tablePath.contains(".")) {
            String[] path = tablePath.split("\\.");

            checkArgument(
                    path.length == 2,
                    String.format(
                            "Table name '%s' is not valid. The parsed length is %d",
                            tablePath, path.length));

            return new OdpsTablePath(path[0], path[1]);
        } else {
            return new OdpsTablePath(defaultProject, tablePath);
        }
    }

    public static String toTablePath(String projectName, String tableName) {
        return new OdpsTablePath(projectName, tableName).getFullPath();
    }

    public String getFullPath() {
        return String.format("%s.%s", projectName, tableName);
    }

    public String getProjectName() {
        return projectName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String toString() {
        return getFullPath();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        OdpsTablePath that = (OdpsTablePath) o;
        return Objects.equals(projectName, that.projectName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projectName, tableName);
    }
}
