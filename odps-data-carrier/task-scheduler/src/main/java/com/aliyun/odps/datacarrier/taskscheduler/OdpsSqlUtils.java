package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Iterator;
import java.util.LinkedList;
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
      // TODO: type should be transformed before this
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
   * Get add partition statements
   *
   * @param tableMetaModel {@link MetaSource.TableMetaModel}
   * @return List of add partition statements. Each could add at most 1000 partitions. If the input
   * does not contain any partition, an empty list will be returned.
   * @throws IllegalArgumentException when input represents a non partitioned table
   */
  public static List<String> getAddPartitionStatement(MetaSource.TableMetaModel tableMetaModel) {
    if (tableMetaModel.partitionColumns.size() == 0) {
      throw new IllegalArgumentException("Not a partitioned table");
    }

    List<String> addPartitionStatements = new LinkedList<>();
    if (tableMetaModel.partitions.size() == 0) {
      return addPartitionStatements;
    }

    Iterator<MetaSource.PartitionMetaModel> iterator = tableMetaModel.partitions.iterator();
    while (iterator.hasNext()) {
      StringBuilder addPartitionBuilder = new StringBuilder();
      StringBuilder dropPartitionBuilder = new StringBuilder();
      dropPartitionBuilder.append("ALTER TABLE\n");
      dropPartitionBuilder.append(tableMetaModel.odpsProjectName)
          .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
      dropPartitionBuilder.append("DROP IF EXISTS");
      addPartitionBuilder.append("ALTER TABLE\n");
      addPartitionBuilder.append(tableMetaModel.odpsProjectName)
          .append(".`").append(tableMetaModel.odpsTableName).append("`\n");
      addPartitionBuilder.append("ADD IF NOT EXISTS");

      for (int i = 0; i < ADD_PARTITION_BATCH_SIZE; i++) {
        if (iterator.hasNext()) {
          MetaSource.PartitionMetaModel partitionMeta = iterator.next();
          String odpsPartitionSpec = getPartitionSpec(tableMetaModel.partitionColumns,
                                                      partitionMeta);
          dropPartitionBuilder.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
          addPartitionBuilder.append("\nPARTITION (").append(odpsPartitionSpec).append(")");
        } else {
          break;
        }
      }
      dropPartitionBuilder.append(";\n");
      addPartitionBuilder.append(";\n");
      addPartitionStatements.add(dropPartitionBuilder.append(addPartitionBuilder).toString());
    }

    return addPartitionStatements;
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
