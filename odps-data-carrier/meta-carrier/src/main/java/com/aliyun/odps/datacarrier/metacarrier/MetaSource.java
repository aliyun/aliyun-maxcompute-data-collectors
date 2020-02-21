package com.aliyun.odps.datacarrier.metacarrier;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MetaSource {

  public static class TableMetaModel {
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
  }

  public static class ColumnMetaModel {
    public String columnName;
    public String odpsColumnName;
    public String type;
    public String odpsType;
    public String comment;
  }

  public static class PartitionMetaModel {
    public List<String> partitionValues = new ArrayList<>();
    public String location;
    public Integer createTime;
  }

  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception;

  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                         String tableName) throws Exception;

  public PartitionMetaModel getPartitionMeta(String databaseName,
                                             String tableName,
                                             List<String> partitionValues) throws Exception;

  /**
   * Get table names in given database
   * @param databaseName database name
   * @return Non-partition tables need to migrate data.
   * @throws Exception
   */
  public List<String> listTables(String databaseName) throws Exception;

  /**
   * Get partition values list of given table
   * @param databaseName database name
   * @param tableName table name
   * @return Partition table left partitions need to migrate data.
   * @throws Exception
   */
   public List<List<String>> listPartitions(String databaseName,
                                            String tableName) throws Exception;

   public List<String> listDatabases() throws Exception;

}
