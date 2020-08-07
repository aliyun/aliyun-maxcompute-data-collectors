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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.utils.StringUtils;

public class HiveMetaSource implements MetaSource {

  private static final Logger LOG = LogManager.getLogger(HiveMetaSource.class);

  private HiveMetaStoreClient hmsClient;

  public HiveMetaSource(String hmsAddr,
                        String principal,
                        String keyTab,
                        List<String> systemProperties) throws MetaException {
    initHmsClient(hmsAddr, principal, keyTab, systemProperties);
  }

  private void initHmsClient(String hmsAddr,
                             String principal,
                             String keyTab,
                             List<String> systemProperties) throws MetaException {
    LOG.info("Initializing HMS client, "
             + "HMS addr: {}, "
             + "kbr principal: {}, "
             + "kbr keytab: {}, "
             + "system properties: {}",
             hmsAddr,
             principal,
             keyTab,
             systemProperties != null ? String.join(" ", systemProperties) : "null");

    Configuration conf = new Configuration();
    //TODO: support user defined hadoop configurations, e.g. HADOOP_RPC_PROTECTION=privacy
    HiveConf hiveConf = new HiveConf(conf, Configuration.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hmsAddr);
    if (!StringUtils.isNullOrEmpty(principal)) {
      LOG.info("Set {} to true", HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
    }
    if (!StringUtils.isNullOrEmpty(keyTab)) {
      LOG.info("Set {} to {}", HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTab);
      hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTab);
    }
    if (systemProperties != null && systemProperties.size() > 0) {
      for (String property : systemProperties) {
        int idx = property.indexOf('=');
        if (idx != -1) {
          LOG.info("Set system property {} = {}",
                   property.substring(0, idx),
                   property.substring(idx + 1));
          System.setProperty(property.substring(0, idx), property.substring(idx + 1));
        } else {
          LOG.error("Invalid system property: " + property);
        }
      }
    }

    this.hmsClient = new HiveMetaStoreClient(hiveConf);
  }

  @Override
  public TableMetaModel getTableMeta(String databaseName, String tableName) throws Exception {
    // Get metadata from hive HMS, ODPS related metadata are not set here
    return getTableMetaInternal(databaseName, tableName, false);
  }

  @Override
  public TableMetaModel getTableMetaWithoutPartitionMeta(String databaseName,
                                                         String tableName) throws Exception {
    return getTableMetaInternal(databaseName, tableName, true);
  }

  private TableMetaModel getTableMetaInternal(String databaseName,
                                              String tableName,
                                              boolean withoutPartitionMeta) throws Exception {
    // Get metadata from hive HMS, notice that ODPS related metadata are not set here
    Table table = hmsClient.getTable(databaseName, tableName);

    TableMetaModel tableMetaModel = new TableMetaModel();
    tableMetaModel.databaseName = databaseName;
    tableMetaModel.tableName = tableName;
    tableMetaModel.location = table.getSd().getLocation();
    tableMetaModel.inputFormat = table.getSd().getInputFormat();
    tableMetaModel.outputFormat = table.getSd().getOutputFormat();
    tableMetaModel.serDe = table.getSd().getSerdeInfo().getSerializationLib();
    if (table.getSd().getSerdeInfo().isSetParameters()) {
      tableMetaModel.serDeProperties.putAll(table.getSd().getSerdeInfo().getParameters());
    }
    if (table.isSetParameters()) {
      Map<String, String> parameters = table.getParameters();
      if (parameters.containsKey("transient_lastDdlTime")) {
        try {
          tableMetaModel.lastModifiedTime =
              Long.parseLong(parameters.get("transient_lastDdlTime"));
        } catch (NumberFormatException ignore) {
        }
      }
    }
    // TODO: get size from hdfs

    List<FieldSchema> columns = hmsClient.getFields(databaseName, tableName);
    for (FieldSchema column : columns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = column.getName();
      columnMetaModel.type = column.getType();
      columnMetaModel.comment = column.getComment();
      tableMetaModel.columns.add(columnMetaModel);
    }

    List<FieldSchema> partitionColumns = table.getPartitionKeys();
    for (FieldSchema partitionColumn : partitionColumns) {
      ColumnMetaModel columnMetaModel = new ColumnMetaModel();
      columnMetaModel.columnName = partitionColumn.getName();
      columnMetaModel.type = partitionColumn.getType();
      columnMetaModel.comment = partitionColumn.getComment();
      tableMetaModel.partitionColumns.add(columnMetaModel);
    }

    // Get partition meta for partitioned tables
    if (!withoutPartitionMeta && partitionColumns.size() > 0) {
      List<Partition> partitions = hmsClient.listPartitions(databaseName, tableName, (short) -1);
      for (Partition partition : partitions) {
        PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
        partitionMetaModel.createTime = (long)partition.getCreateTime();
        partitionMetaModel.location = partition.getSd().getLocation();
        partitionMetaModel.partitionValues = partition.getValues();
        tableMetaModel.partitions.add(partitionMetaModel);
      }
    }

    return tableMetaModel;
  }

  @Override
  public PartitionMetaModel getPartitionMeta(String databaseName, String tableName,
                                             List<String> partitionValues) throws Exception {
    PartitionMetaModel partitionMetaModel = new PartitionMetaModel();
    Partition partition = hmsClient.getPartition(databaseName, tableName, partitionValues);
    partitionMetaModel.createTime = (long) partition.getCreateTime();
    if (partition.isSetParameters()) {
      Map<String, String> parameters = partition.getParameters();
      if (parameters.containsKey("transient_lastDdlTime")) {
        try {
          partitionMetaModel.lastModifiedTime =
              Long.parseLong(parameters.get("transient_lastDdlTime"));
        } catch (NumberFormatException ignore) {
        }
      }
    }
    partitionMetaModel.location = partition.getSd().getLocation();
    partitionMetaModel.partitionValues = partition.getValues();

    return partitionMetaModel;
  }

  @Override
  public boolean hasDatabase(String databaseName) throws Exception {
    try {
      hmsClient.getDatabase(databaseName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasTable(String databaseName, String tableName) throws Exception {
    try {
      hmsClient.getTable(databaseName, tableName);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasPartition(String databaseName, String tableName, List<String> partitionValues)
      throws Exception {
    try {
      hmsClient.getPartition(databaseName, tableName, partitionValues);
    } catch (NoSuchObjectException e) {
      return false;
    }
    return true;
  }

  @Override
  public List<String> listDatabases() throws Exception {
    return hmsClient.getAllDatabases();
  }


  @Override
  public List<String> listTables(String databaseName) throws Exception {
    return hmsClient.getAllTables(databaseName);
  }

  @Override
  public List<List<String>> listPartitions(String databaseName, String tableName) throws Exception {
    List<List<String>> partitionValuesList = new LinkedList<>();
    List<Partition> partitions = hmsClient.listPartitions(databaseName, tableName, (short) -1);
    for (Partition partition : partitions) {
      partitionValuesList.add(partition.getValues());
    }
    return partitionValuesList;
  }

  @Override
  public void shutdown() {
    hmsClient.close();
  }
}
