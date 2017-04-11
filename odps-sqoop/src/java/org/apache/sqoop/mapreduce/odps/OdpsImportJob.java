/**
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
package org.apache.sqoop.mapreduce.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.lib.FieldMapProcessor;
import org.apache.sqoop.mapreduce.DataDrivenImportJob;
import org.apache.sqoop.mapreduce.DelegatingOutputFormat;
import org.apache.sqoop.odps.OdpsConstants;
import org.apache.sqoop.odps.OdpsUploadProcessor;
import org.apache.sqoop.odps.OdpsUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Tian Li on 15/9/29.
 */
public class OdpsImportJob extends DataDrivenImportJob {
  public static final Log LOG
          = LogFactory.getLog(OdpsImportJob.class.getName());

  public OdpsImportJob(final SqoopOptions opts,
                       final ImportJobContext context) {
    super(opts, context.getInputFormat(), context);
  }

  @Override
  protected void configureMapper(Job job, String tableName,
                                 String tableClassName) {
    job.setOutputKeyClass(SqoopRecord.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapperClass(getMapperClass());
  }

  @Override
  protected Class<? extends Mapper> getMapperClass() {
    return OdpsImportMapper.class;
  }

  @Override
  protected Class<? extends OutputFormat> getOutputFormatClass()
          throws ClassNotFoundException {
    return DelegatingOutputFormat.class;
  }

  @Override
  protected void configureOutputFormat(Job job, String tableName,
                                       String tableClassName)
          throws ClassNotFoundException {
    job.setOutputFormatClass(getOutputFormatClass());
    Configuration conf = job.getConfiguration();
    conf.setClass("sqoop.output.delegate.field.map.processor.class",
            OdpsUploadProcessor.class,
            FieldMapProcessor.class);

    conf.setStrings(OdpsConstants.INPUT_COL_NAMES, getColumnNames());

    String odpsTableName = options.getOdpsTable();
    if (odpsTableName == null) {
      odpsTableName = tableName;
    }
    conf.set(OdpsConstants.TABLE_NAME, odpsTableName);
    conf.set(OdpsConstants.ACCESS_ID, options.getOdpsAccessID());
    conf.set(OdpsConstants.ACCESS_KEY, options.getOdpsAccessKey());
    conf.set(OdpsConstants.ENDPOINT, options.getOdpsEndPoint());

    String tunnelEndPoint = options.getOdpsTunnelEndPoint();
    if (tunnelEndPoint != null) {
      conf.set(OdpsConstants.TUNNEL_ENDPOINT,
              options.getOdpsTunnelEndPoint());
    }

    conf.set(OdpsConstants.PROJECT, options.getOdpsProject());

    String partitionKey = options.getOdpsPartitionKey();
    String partitionValue = options.getOdpsPartitionValue();
    if (partitionKey != null && partitionValue != null) {
      conf.set(OdpsConstants.PARTITION_KEY, partitionKey);
      conf.set(OdpsConstants.PARTITION_VALUE, partitionValue);
    }
    conf.setBoolean(OdpsConstants.CREATE_TABLE,
            options.isOdpsCreateTable());
    String dateFormat = options.getOdpsInputDateFormat();
    if (dateFormat != null) {
      conf.set(OdpsConstants.DATE_FORMAT, dateFormat);
    }
    conf.setInt(OdpsConstants.RETRY_COUNT, options.getOdpsRetryCount());
    conf.setInt(OdpsConstants.BATCH_SIZE, options.getOdpsBatchSize());
  }

  private String [] getColumnNames() {
    String [] colNames = options.getColumns();
    String tableName = options.getTableName();
    if (tableName == null) {
      tableName = getContext().getTableName();
    }
    if (null != colNames) {
      return colNames; // user-specified column names.
    } else if (null != tableName) {
      return getContext().getConnManager().getColumnNames(tableName);
    } else {
      return getContext().getConnManager().
              getColumnNamesForQuery(options.getSqlQuery());
    }
  }

  @Override
  protected void jobSetup(Job job) throws IOException, ImportException {
    boolean isCreateTable = options.isOdpsCreateTable();

    String tableName = Preconditions.checkNotNull(options.getOdpsTable(),
            "Import to ODPS error: Table name not specified");
    String accessID = Preconditions.checkNotNull(options.getOdpsAccessID(),
            "Error: ODPS access ID not specified");
    String accessKey = Preconditions.checkNotNull(options.getOdpsAccessKey(),
            "Error: ODPS access key not specified");
    String project = Preconditions.checkNotNull(options.getOdpsProject(),
            "Error: ODPS project not specified");
    String endpoint = Preconditions.checkNotNull(options.getOdpsEndPoint(),
            "Error: ODPS endpoint not specified");

    Odps odps = new Odps(new AliyunAccount(accessID, accessKey));
    odps.setUserAgent(OdpsUtil.getUserAgent());
    odps.setDefaultProject(project);
    odps.setEndpoint(endpoint);

    Tables tables = odps.tables();
    boolean existsTable;
    try {
      existsTable = tables.exists(tableName);
    } catch (OdpsException e) {
      throw new ImportException("ODPS exception", e);
    }

    Map<String, OdpsType> colTypeMap = getColTypeMap();

    if(!existsTable) {
      if (!isCreateTable) {
        LOG.warn("Could not find ODPS table " + tableName);
        LOG.warn("This job may fail. Either explicitly create the table,");
        LOG.warn("or re-run with --odps-create-table.");
      } else {
        TableSchema schema = new TableSchema();
        for(Map.Entry<String, OdpsType> colTypeEntry
                : colTypeMap.entrySet()) {
          schema.addColumn(new Column(colTypeEntry.getKey(),
                  colTypeEntry.getValue()));
        }
        String partitionColumns = options.getOdpsPartitionKey();
        if(StringUtils.isNotEmpty(partitionColumns)) {
          String[] partitionCols = partitionColumns.split(",");
          for (int i = 0; i < partitionCols.length; i++) {
            schema.addPartitionColumn(new Column(partitionCols[i].trim(),
                    OdpsType.STRING));
          }
        }
        try {
          tables.create(tableName, schema);
          if (options.getOdpsDatahubEndPoint() != null) {
            int shardNum = options.getOdpsShardNum();
            int hubLifeCycle = options.getOdpsHubLifeCycle();
//            tables.get(tableName).createShards(shardNum, true, hubLifeCycle);
            tables.get(tableName).createShards(shardNum);
          }
        } catch (OdpsException e) {
          throw new ImportException("Create table failed", e);
        }
      }
    } else {
      TableSchema schema = tables.get(tableName).getSchema();
      for(Map.Entry<String, OdpsType> colTypeEntry: colTypeMap.entrySet()) {
        Column column = schema.getColumn(colTypeEntry.getKey().toLowerCase());
        if (column.getType() != colTypeEntry.getValue()) {
          throw new ImportException("Column type of ODPS table not"
            + " consistent with user specification. Column name: "
            + colTypeEntry.getKey());
        }
      }
    }
  }

  private Map<String, OdpsType> getColTypeMap() {
    Map colTypeMap = Maps.newLinkedHashMap();
    Properties userMapping = options.getMapColumnOdps();
    String[] columnNames = getColumnNames();

    for(Object column : userMapping.keySet()) {
      boolean found = false;
      for(String c : columnNames) {
        if (c.equals(column)) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new IllegalArgumentException("No column by the name " + column
                + "found while importing data");
      }
    }
    for(String col: columnNames) {
      String userType = userMapping.getProperty(col);
      if(userType != null) {
        colTypeMap.put(col, OdpsType.valueOf(userType));
      } else {
        LOG.warn("Column " + col
                + " type not specified, defaulting to STRING.");
        colTypeMap.put(col, OdpsType.STRING);
      }
    }
    return colTypeMap;
  }
}
