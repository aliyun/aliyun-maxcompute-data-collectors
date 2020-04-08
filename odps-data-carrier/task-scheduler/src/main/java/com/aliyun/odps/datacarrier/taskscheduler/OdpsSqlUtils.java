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

import java.util.List;


public class OdpsSqlUtils {

  public static final int ADD_PARTITION_BATCH_SIZE = 1000;

  public static String getDropTableStatement(MetaSource.TableMetaModel tableMetaModel) {

    return "DROP TABLE IF EXISTS " + tableMetaModel.odpsProjectName
           + ".`" + tableMetaModel.odpsTableName + "`;\n";
  }

  public static String getCreateTableStatement(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();

    sb
        .append("CREATE TABLE IF NOT EXISTS ")
        .append(tableMetaModel.odpsProjectName).append(".")
        .append("`").append(tableMetaModel.odpsTableName).append("` (\n");

    for (int i = 0; i < tableMetaModel.columns.size(); i++) {
      MetaSource.ColumnMetaModel columnMetaModel = tableMetaModel.columns.get(i);
      sb.append("    `").append(columnMetaModel.odpsColumnName).append("` ")
          .append(columnMetaModel.odpsType);

      if (columnMetaModel.comment != null) {
        sb.append(" COMMENT '").append(columnMetaModel.comment).append("'");
      }

      if (i + 1 < tableMetaModel.columns.size()) {
        sb.append(",\n");
      }
    }

    sb.append("\n)");

    if (tableMetaModel.comment != null) {
      sb.append("\nCOMMENT '").append(tableMetaModel.comment).append("'\n");
    }

    if (tableMetaModel.partitionColumns.size() > 0) {
      sb.append("\nPARTITIONED BY (\n");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel partitionColumnMetaModel = tableMetaModel.partitionColumns.get(i);
        sb.append("    `").append(partitionColumnMetaModel.odpsColumnName).append("` ")
            .append(partitionColumnMetaModel.odpsType);

        if (partitionColumnMetaModel.comment != null) {
          sb.append(" COMMENT '").append(partitionColumnMetaModel.comment).append("'");
        }

        if (i + 1 < tableMetaModel.partitionColumns.size()) {
          sb.append(",\n");
        }
      }
      sb.append("\n)");
    }

    sb.append(";\n");

    return sb.toString();
  }

  /**
   * Get drop partition statement
   *
   * @param tableMetaModel {@link MetaSource.TableMetaModel}
   * @return Drop partition statement for multiple partitions
   * @throws IllegalArgumentException when input represents a non partitioned table
   */
  public static String getDropPartitionStatement(MetaSource.TableMetaModel tableMetaModel) {
    if (tableMetaModel.partitionColumns.size() == 0) {
      throw new IllegalArgumentException("Not a partitioned table");
    }

    if (tableMetaModel.partitions.size() > ADD_PARTITION_BATCH_SIZE) {
      throw new IllegalArgumentException(
          "Partition batch size exceeds upper bound: " + ADD_PARTITION_BATCH_SIZE);
    }

    StringBuilder sb = new StringBuilder();
    if (tableMetaModel.partitions.size() == 0) {
      return sb.toString();
    }

    sb.append("ALTER TABLE\n");
    sb.append(tableMetaModel.odpsProjectName)
        .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
    sb.append("DROP IF EXISTS");
    for (int i = 0; i < tableMetaModel.partitions.size(); i++) {
      MetaSource.PartitionMetaModel partitionMetaModel = tableMetaModel.partitions.get(i);
      String odpsPartitionSpec = getPartitionSpec(tableMetaModel.partitionColumns,
                                                  partitionMetaModel);
      sb.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
      if (i != tableMetaModel.partitions.size() - 1) {
        sb.append(",");
      }
    }
    sb.append(";\n");

    return sb.toString();
  }

  /**
   * Get add partition statement
   *
   * @param tableMetaModel {@link MetaSource.TableMetaModel}
   * @return Add partition statement for multiple partitions
   * @throws IllegalArgumentException when input represents a non partitioned table
   */
  public static String getAddPartitionStatement(MetaSource.TableMetaModel tableMetaModel) {
    if (tableMetaModel.partitionColumns.size() == 0) {
      throw new IllegalArgumentException("Not a partitioned table");
    }

    if (tableMetaModel.partitions.size() > ADD_PARTITION_BATCH_SIZE) {
      throw new IllegalArgumentException(
          "Partition batch size exceeds upper bound: " + ADD_PARTITION_BATCH_SIZE);
    }

    StringBuilder sb = new StringBuilder();
    if (tableMetaModel.partitions.size() == 0) {
      return sb.toString();
    }

    sb.append("ALTER TABLE\n");
    sb.append(tableMetaModel.odpsProjectName)
        .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
    sb.append("ADD IF NOT EXISTS");
    for (MetaSource.PartitionMetaModel partitionMetaModel : tableMetaModel.partitions) {
      String odpsPartitionSpec = getPartitionSpec(tableMetaModel.partitionColumns,
                                                  partitionMetaModel);
      sb.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
    }
    sb.append(";\n");

    return sb.toString();
  }

  public static String getVerifySql(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");

    if (tableMetaModel.partitionColumns.size() > 0) {
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel columnMetaModel = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(columnMetaModel.odpsColumnName).append("`");
        sb.append(", ");
      }
    }

    sb.append("COUNT(1) FROM\n");
    sb.append(tableMetaModel.odpsProjectName)
        .append(".`").append(tableMetaModel.odpsTableName).append("`\n");

    if (tableMetaModel.partitionColumns.size() > 0) {
      String whereCondition = getWhereCondition(tableMetaModel);
      sb.append(whereCondition);

      sb.append("\nGROUP BY ");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.odpsColumnName).append("`");
        if (i != tableMetaModel.partitionColumns.size() - 1) {
          sb.append(", ");
        }
      }

      sb.append("\nORDER BY ");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.odpsColumnName).append("`");
        if (i != tableMetaModel.partitionColumns.size() - 1) {
          sb.append(", ");
        }
      }

      sb
          .append("\nLIMIT")
          .append(" ")
          .append(tableMetaModel.partitions.size());
    }
    sb.append(";\n");

    return sb.toString();
  }

  private static String getPartitionSpec(List<MetaSource.ColumnMetaModel> partitionColumns,
                                         MetaSource.PartitionMetaModel partitionMetaModel) {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < partitionColumns.size(); i++) {
      MetaSource.ColumnMetaModel partitionColumn = partitionColumns.get(i);
      String partitionValue = partitionMetaModel.partitionValues.get(i);

      sb.append(partitionColumn.odpsColumnName).append("=");
      if ("STRING".equalsIgnoreCase(partitionColumn.odpsType)) {
        sb.append("'").append(partitionValue).append("'");
      } else {
        // TODO: __HIVE_DEFAULT_PARTITION__ should be handled before this
        sb.append(partitionValue);
      }
      if (i != partitionColumns.size() - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }

  private static String getWhereCondition(MetaSource.TableMetaModel tableMetaModel) {
    if (tableMetaModel == null) {
      throw new IllegalArgumentException("'tableMetaModel' cannot be null");
    }

    StringBuilder sb = new StringBuilder();

    // Return if this is not a partitioned table
    if (tableMetaModel.partitionColumns.size() == 0) {
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

      sb.append(partitionColumn.odpsColumnName).append("=");
      if ("STRING".equalsIgnoreCase(partitionColumn.odpsType)) {
        sb.append("'").append(partitionValue).append("'");
      } else {
        sb.append(partitionValue);
      }
      if (i != partitionColumns.size() - 1) {
        sb.append(" AND ");
      }
    }

    return sb.toString();
  }
}
