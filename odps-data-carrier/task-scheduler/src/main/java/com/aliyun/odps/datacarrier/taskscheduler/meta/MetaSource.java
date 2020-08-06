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

package com.aliyun.odps.datacarrier.taskscheduler.meta;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MetaSource {

  class TableMetaModel implements Cloneable {

    public String databaseName;
    public String odpsProjectName;
    public String tableName;
    public String odpsTableName;
    public String odpsTableStorage; // used for create external table, specify destination table storage, such as OSS
    public String comment;
    public Long size; // in Byte
    public String location;
    public String inputFormat;
    public String outputFormat;
    public String serDe;
    public Long createTime;
    public Long lastModifiedTime; // in second
    public Map<String, String> serDeProperties = new LinkedHashMap<>();
    public List<ColumnMetaModel> columns = new ArrayList<>();
    public List<ColumnMetaModel> partitionColumns = new ArrayList<>();
    public List<PartitionMetaModel> partitions = new ArrayList<>();

    // TODO: not table properties, move to migration config later
    public Integer lifeCycle;
    public Boolean ifNotExists = true;
    public Boolean dropIfExists = true;

    @Override
    public TableMetaModel clone() {
      TableMetaModel tableMetaModel = new TableMetaModel();
      tableMetaModel.databaseName = this.databaseName;
      tableMetaModel.odpsProjectName = this.odpsProjectName;
      tableMetaModel.tableName = this.tableName;
      tableMetaModel.odpsTableName = this.odpsTableName;
      tableMetaModel.odpsTableStorage = this.odpsTableStorage;
      tableMetaModel.comment = this.comment;
      tableMetaModel.size = this.size;
      tableMetaModel.location = this.location;
      tableMetaModel.inputFormat = this.inputFormat;
      tableMetaModel.outputFormat = this.outputFormat;
      tableMetaModel.createTime = this.createTime;
      tableMetaModel.lastModifiedTime = this.lastModifiedTime;
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
    public Long createTime;
    public Long lastModifiedTime;
  }

  /**
   * Check database existence
   *
   * @param databaseName Database name
   * @return True if the database exists, else false
   * @throws Exception
   */
  boolean hasDatabase(String databaseName) throws Exception;

  /**
   * Check table existence
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return True if the table exists, else false
   * @throws Exception
   */
  boolean hasTable(String databaseName, String tableName) throws Exception;

  /**
   * Check partition existence
   * @param databaseName Database name
   * @param tableName Table name
   * @param partitionValues partition values
   * @return True if the partition exists, else false
   * @throws Exception
   */
  boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception;

  /**
   * Get database list
   *
   * @return List of database
   * @throws Exception
   */
  List<String> listDatabases() throws Exception;

  /**
   * Get table names in given database
   *
   * @param databaseName Database name
   * @return List of table names, include views
   * @throws Exception
   */
  List<String> listTables(String databaseName) throws Exception;

  /**
   * Get partition list of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return List of partition values of specified table
   * @throws Exception
   */
  List<List<String>> listPartitions(String databaseName,
                                    String tableName) throws Exception;

  /**
   * Get metadata of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return Metadata of specified table
   * @throws Exception
   */
  TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception;

  /**
   * Get metadata of specified table
   *
   * @param databaseName Database name
   * @param tableName    Table name
   * @return Metadata of specified table, partition metadata not included
   * @throws Exception
   */
  TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                  String tableName) throws Exception;

  /**
   * Get metadata of specified partition
   *
   * @param databaseName    Database name
   * @param tableName       Table name
   * @param partitionValues Partition values
   * @return Metadata of specified partition
   * @throws Exception
   */
  PartitionMetaModel getPartitionMeta(String databaseName,
                                      String tableName,
                                      List<String> partitionValues) throws Exception;

  /**
   * Shutdown
   */
  void shutdown();
}
