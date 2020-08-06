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

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class OdpsSqlUtils {

  public static String getDropTableStatement(String db, String tb) {
    return "DROP TABLE IF EXISTS " + db + ".`" + tb + "`;\n";
  }

  public static String getCreateTableStatement(MetaSource.TableMetaModel tableMetaModel) {
    return getCreateTableStatement(tableMetaModel, null);
  }

  public static String getCreateTableStatement(MetaSource.TableMetaModel tableMetaModel,
                                               ExternalTableConfig externalTableConfig) {
    StringBuilder sb = new StringBuilder();
    if (externalTableConfig != null) {
      sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
    } else {
      sb.append("CREATE TABLE IF NOT EXISTS ");
    }
    sb.append(tableMetaModel.odpsProjectName).append(".")
        .append("`").append(tableMetaModel.odpsTableName).append("`");
    sb.append(getCreateTableStatementWithoutDatabaseName(tableMetaModel, externalTableConfig));
    return sb.toString();
  }

  public static String getCreateTableStatementWithoutDatabaseName(MetaSource.TableMetaModel tableMetaModel,
                                                                  ExternalTableConfig externalTableConfig) {
    StringBuilder sb = new StringBuilder("(\n");
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

    if (externalTableConfig != null) {
      switch (externalTableConfig.getStorage()) {
        case OSS:
          sb.append(getCreateOssExternalTableCondition(tableMetaModel, externalTableConfig));
          break;
        default:
          throw new IllegalArgumentException("Unknown external table storage: " + externalTableConfig.getStorage().name());
      }
    }

    sb.append(";\n");

    return sb.toString();
  }

  private static String getCreateOssExternalTableCondition(MetaSource.TableMetaModel tableMetaModel,
                                                           ExternalTableConfig externalTableConfig) {
    StringBuilder sb = new StringBuilder();
    OssExternalTableConfig ossExternalTableConfig = (OssExternalTableConfig) externalTableConfig;

    if (!StringUtils.isNullOrEmpty(ossExternalTableConfig.getRoleRan())) {
      tableMetaModel.serDeProperties.put("odps.properties.rolearn",
                                         ossExternalTableConfig.getRoleRan());
    }

    sb.append("ROW FORMAT serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'\n");
    if (MapUtils.isNotEmpty(tableMetaModel.serDeProperties)) {
      sb.append("WITH SERDEPROPERTIES (").append("\n");
      List<String> propertyStrings = new LinkedList<>();
      for (Map.Entry<String, String> property : tableMetaModel.serDeProperties.entrySet()) {
        if (!StringEscapeUtils.escapeJava(property.getValue()).startsWith("\\u")) {
          String propertyString = String.format("'%s'='%s'",
              StringEscapeUtils.escapeJava(property.getKey()),
              StringEscapeUtils.escapeJava(property.getValue()));
          propertyStrings.add(propertyString);
        }
      }
      sb.append(String.join(",\n", propertyStrings)).append(")\n");
    }

    // TODO: support other formats
    sb.append("STORED AS INPUTFORMAT\n");
    sb.append("'org.apache.hadoop.hive.ql.io.RCFileInputFormat'\n");
    sb.append("OUTPUTFORMAT\n");
    sb.append("'org.apache.hadoop.hive.ql.io.RCFileOutputFormat'\n");
    sb.append("LOCATION '").append(externalTableConfig.getLocation()).append("';\n");
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

    if (tableMetaModel.partitions.size() > Constants.DEFAULT_PARTITION_BATCH_SIZE) {
      throw new IllegalArgumentException(
          "Partition batch size exceeds upper bound: " + Constants.DEFAULT_PARTITION_BATCH_SIZE);
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

    if (tableMetaModel.partitions.size() > Constants.DEFAULT_PARTITION_BATCH_SIZE) {
      throw new IllegalArgumentException(
          "Partition batch size exceeds upper bound: " + Constants.DEFAULT_PARTITION_BATCH_SIZE);
    }

    StringBuilder sb = new StringBuilder();
    if (tableMetaModel.partitions.size() == 0) {
      return sb.toString();
    }

    sb.append("ALTER TABLE\n");
    sb.append(tableMetaModel.odpsProjectName)
        .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
    sb.append(getAddPartitionStatementWithoutDatabaseName(tableMetaModel));
    return sb.toString();
  }

  public static String getAddPartitionStatementWithoutDatabaseName(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("ADD IF NOT EXISTS");
    for (MetaSource.PartitionMetaModel partitionMetaModel : tableMetaModel.partitions) {
      String odpsPartitionSpec = getPartitionSpec(tableMetaModel.partitionColumns, partitionMetaModel);
      sb.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
    }
    sb.append(";\n");

    return sb.toString();
  }

  public static String getInsertOverwriteTableStatement(MetaSource.TableMetaModel tableMetaModel) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT OVERWRITE TABLE ")
        .append(tableMetaModel.odpsProjectName)
        .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
    if (!tableMetaModel.partitionColumns.isEmpty()) {
      sb.append("PARTITION (");
      for (int i = 0; i < tableMetaModel.partitionColumns.size(); i++) {
        MetaSource.ColumnMetaModel c = tableMetaModel.partitionColumns.get(i);
        sb.append("`").append(c.columnName).append("`");
        if (i != tableMetaModel.partitionColumns.size() - 1) {
          sb.append(", ");
        }
      }
      sb.append(")\n");
    }
    sb.append("SELECT * FROM ")
        .append(tableMetaModel.databaseName)
        .append(".`").append(tableMetaModel.tableName).append("`").append("\n");
    sb.append(getWhereCondition(tableMetaModel));
    sb.append(";\n");
    return sb.toString();
  }

  public static String getVerifySql(MetaSource.TableMetaModel tableMetaModel) {
    return getVerifySql(tableMetaModel, true);
  }

  public static String getVerifySql(MetaSource.TableMetaModel tableMetaModel, boolean verifyDestinationTable) {
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
    String database;
    String table;
    if (verifyDestinationTable) {
      database = tableMetaModel.odpsProjectName;
      table = tableMetaModel.odpsTableName;
    } else {
      database = tableMetaModel.databaseName;
      table = tableMetaModel.tableName;
    }
    sb.append(database).append(".`").append(table).append("`\n");

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

  public static String getDDLSql(MetaSource.TableMetaModel tableMetaModel) {
    return "SHOW CREATE TABLE " + tableMetaModel.databaseName
           + ".`" + tableMetaModel.tableName + "`;\n";
  }


  public static String getDropViewStatement(String db, String tbl) {
    return "DROP VIEW IF EXISTS " + db + ".`" + tbl + "`;\n";
  }

  public static String getCreateViewStatement(String db, String tbl, String viewText) {
    return "CREATE VIEW IF NOT EXISTS " + db + ".`" + tbl + "` AS " + viewText + ";\n";
  }

  public static String getAddResourceStatement(String resourceType, String filePath, String resourceName, String comment) {
    String result = "ADD " + resourceType + " " + filePath + " AS " + resourceName;
    if (!StringUtils.isNullOrEmpty(comment)) {
      result = result + " COMMENT '" + comment + "'";
    }
    return result + ";\n";
  }

  public static String getCreateFunctionStatement(String name,
                                                  String classPath,
                                                  List<String> resources) {
    return "CREATE FUNCTION " + name + " as '" + classPath +
        "' USING '" + String.join(",", resources) + "';\n";
  }

  /**
   * Get oss folder to store external table data
   * @param ossEndpoint
   * @param ossBucket
   * @param ossFilePath relative path from bucket, such as a/b/
   * @return
   */
  public static String getOssTablePath(String ossEndpoint,
                                       String ossBucket,
                                       String ossFilePath) {
    if (StringUtils.isNullOrEmpty(ossEndpoint)
        || StringUtils.isNullOrEmpty(ossBucket)) {
      throw new IllegalArgumentException("Undefined OSS endpoint or OSS bucket");
    }
    StringBuilder locationBuilder = new StringBuilder();
    if (!ossEndpoint.startsWith("oss://")) {
      locationBuilder.append("oss://");
    }
    locationBuilder.append(ossEndpoint);
    if (!ossEndpoint.endsWith("/")) {
      locationBuilder.append("/");
    }
    locationBuilder.append(ossBucket);
    if (!ossBucket.endsWith("/")) {
      locationBuilder.append("/");
    }
    locationBuilder.append(ossFilePath);
    if (!ossFilePath.endsWith("/")) {
      locationBuilder.append("/");
    }
    return locationBuilder.toString();
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
