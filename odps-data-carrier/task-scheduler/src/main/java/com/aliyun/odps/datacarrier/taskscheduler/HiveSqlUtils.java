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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;

public class HiveSqlUtils {

  public static String getUdtfSql(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();

    List<String> hiveColumnNames = new ArrayList<>();
    List<String> odpsColumnNames = new ArrayList<>();
    for (MetaSource.ColumnMetaModel columnMeta : tableMetaModel.columns) {
      odpsColumnNames.add(columnMeta.odpsColumnName);
      hiveColumnNames.add(columnMeta.columnName);
    }
    List<String> odpsPartitionColumnNames = new ArrayList<>();
    for (MetaSource.ColumnMetaModel columnMeta : tableMetaModel.partitionColumns) {
      odpsPartitionColumnNames.add(columnMeta.odpsColumnName);
      hiveColumnNames.add(columnMeta.columnName);
    }

    sb.append("SELECT odps_data_dump_multi(\n")
        .append("'").append(tableMetaModel.odpsProjectName).append("',\n")
        .append("'").append(tableMetaModel.odpsTableName).append("',\n")
        .append("'").append(String.join(",", odpsColumnNames)).append("',\n")
        .append("'").append(String.join(",", odpsPartitionColumnNames)).append("',\n");

    for (int i = 0; i < hiveColumnNames.size(); i++) {
      sb.append("`").append(hiveColumnNames.get(i)).append("`");
      if (i != hiveColumnNames.size() - 1) {
        sb.append(",\n");
      } else {
        sb.append(")\n");
      }
    }

    String databaseName = tableMetaModel.databaseName;
    String tableName = tableMetaModel.tableName;
    sb.append("FROM ")
        .append(databaseName).append(".`").append(tableName).append("`").append("\n");
    String whereCondition = getWhereCondition(tableMetaModel);
    sb.append(whereCondition);
    return sb.toString();
  }

  public static String getVerifySql(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");

    if (tableMetaModel.partitionColumns.size() > 0) {
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.columnName).append("`");
        sb.append(", ");
      }
    }

    sb.append("COUNT(1) FROM\n");
    sb.append(tableMetaModel.databaseName)
        .append(".`").append(tableMetaModel.tableName).append("`\n");

    if (tableMetaModel.partitionColumns.size() > 0) {

      // Add where condition
      String whereCondition = getWhereCondition(tableMetaModel);
      sb.append(whereCondition);

      sb.append("\nGROUP BY ");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.columnName).append("`");
        if (i != tableMetaModel.partitionColumns.size() - 1) {
          sb.append(", ");
        }
      }

      sb.append("\nORDER BY ");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.columnName).append("`");
        if (i != tableMetaModel.partitionColumns.size() - 1) {
          sb.append(", ");
        }
      }
    }
    sb.append("\n");
    return sb.toString();
  }

  private static String getWhereCondition(MetaSource.TableMetaModel tableMetaModel) {
    if (tableMetaModel == null) {
      throw new IllegalArgumentException("'tableMetaModel' cannot be null");
    }

    StringBuilder sb = new StringBuilder();

    // Return if this is not a partitioned table
    if (tableMetaModel.partitionColumns.size() == 0 || tableMetaModel.partitions.isEmpty()) {
      return sb.toString();
    }

    sb.append("WHERE\n");
    for (int i = 0; i < tableMetaModel.partitions.size(); i++) {
      String entry = getWhereConditionEntry(tableMetaModel.partitionColumns,
                                            tableMetaModel.partitions.get(i));
      sb.append(entry);

      if (i != tableMetaModel.partitions.size() - 1) {
        sb.append(" OR\n");
      }
    }

    sb.append("\n");
    return sb.toString();
  }

  private static String getWhereConditionEntry(List<MetaSource.ColumnMetaModel> partitionColumns,
                                               MetaSource.PartitionMetaModel partitionMetaModel) {
    if (partitionColumns == null || partitionMetaModel == null) {
      throw new IllegalArgumentException(
          "'partitionColumns' or 'partitionMetaModel' cannot be null");
    }

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      MetaSource.ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = partitionMetaModel.partitionValues.get(i);

      sb.append(partitionColumn.columnName).append("=").append("cast('").append(partitionValue)
          .append("' AS ").append(partitionColumn.type).append(")");

      if (i != partitionColumns.size() - 1) {
        sb.append(" AND ");
      }
    }

    return sb.toString();
  }
}
