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


  boolean hasTable(String databaseName, String tableName) throws Exception;

  boolean hasDatabase(String databaseName) throws Exception;

  /**
   * Get table names in given database
   * @param databaseName database name
   * @return Non-partition tables need to migrate data.
   * @throws Exception
   */
  List<String> listTables(String databaseName) throws Exception;

  TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception;

  TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                  String tableName) throws Exception;

  /**
   * Get partition values list of given table
   * @param databaseName database name
   * @param tableName table name
   * @return Partition table left partitions need to migrate data.
   * @throws Exception
   */
  List<List<String>> listPartitions(String databaseName,
                                    String tableName) throws Exception;

  PartitionMetaModel getPartitionMeta(String databaseName,
                                      String tableName,
                                      List<String> partitionValues) throws Exception;

  List<String> listDatabases() throws Exception;

   void shutdown();

}
