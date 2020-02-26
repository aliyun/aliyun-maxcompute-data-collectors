package com.aliyun.odps.datacarrier.metacarrier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MetaSource {

  class TableMetaModel {
    public String databaseName;
    public String odpsProjectName;
    public String tableName;
    public String odpsTableName;
    public String comment;
    public Long size;
    public String location;
    public String inputFormat;
    public String outputFormat;
    public String serDe;
    public Map<String, String> serDeProperties = new LinkedHashMap<>();
    public List<ColumnMetaModel> columns = new ArrayList<>();
    public List<ColumnMetaModel> partitionColumns = new ArrayList<>();
    public List<PartitionMetaModel> partitions = new ArrayList<>();

    // TODO: not table properties, move to migration config later
    public Integer lifeCycle;
    public Boolean ifNotExists = true;
    public Boolean dropIfExists = true;

    public TableMetaModel createSubTableMetaModel(List<PartitionMetaModel> partitions) {
      TableMetaModel tableMetaModel = new TableMetaModel();
      tableMetaModel.databaseName = this.databaseName;
      tableMetaModel.odpsProjectName = this.odpsProjectName;
      tableMetaModel.tableName = this.tableName;
      tableMetaModel.odpsTableName = this.odpsTableName;
      tableMetaModel.comment = this.comment;
      tableMetaModel.size = this.size;
      tableMetaModel.location = this.location;
      tableMetaModel.inputFormat = this.inputFormat;
      tableMetaModel.outputFormat = this.outputFormat;
      tableMetaModel.serDeProperties = this.serDeProperties;
      tableMetaModel.columns = this.columns;
      tableMetaModel.partitionColumns = this.partitionColumns;
      tableMetaModel.lifeCycle = this.lifeCycle;
      tableMetaModel.ifNotExists = this.ifNotExists;
      tableMetaModel.dropIfExists = this.dropIfExists;
      tableMetaModel.partitions = partitions;
      return tableMetaModel;
    }
  }

  class ColumnMetaModel {
    public String columnName;
    public String odpsColumnName;
    public String type;
    public String odpsType;
    public String comment;
  }

  class PartitionMetaModel {
    public List<String> partitionValues = new ArrayList<>();
    public String location;
    public Integer createTime;
  }

  TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception;

  TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                         String tableName) throws Exception;

  PartitionMetaModel getPartitionMeta(String databaseName,
                                             String tableName,
                                             List<String> partitionValues) throws Exception;

  /**
   * Get table names in given database
   * @param databaseName database name
   * @return Non-partition tables need to migrate data.
   * @throws Exception
   */
  List<String> listTables(String databaseName) throws Exception;

  /**
   * Get partition values list of given table
   * @param databaseName database name
   * @param tableName table name
   * @return Partition table left partitions need to migrate data.
   * @throws Exception
   */
   List<List<String>> listPartitions(String databaseName,
                                            String tableName) throws Exception;

   List<String> listDatabases() throws Exception;

}
