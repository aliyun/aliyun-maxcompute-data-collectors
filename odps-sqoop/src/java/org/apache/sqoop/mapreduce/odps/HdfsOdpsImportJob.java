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
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.JobBase;
import com.cloudera.sqoop.orm.TableClassName;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.sqoop.lib.FieldMapProcessor;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.mapreduce.DelegatingOutputFormat;
import org.apache.sqoop.mapreduce.ExportInputFormat;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.TextExportMapper;
import org.apache.sqoop.odps.OdpsConstants;
import org.apache.sqoop.odps.OdpsUploadProcessor;
import org.apache.sqoop.odps.OdpsUtil;
import org.apache.sqoop.util.PerfCounters;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class HdfsOdpsImportJob extends JobBase {
  public static final String SQOOP_EXPORT_MAP_TASK_MAX_ATTEMTPS =
      "sqoop.export.mapred.map.max.attempts";

  private static final String HADOOP_MAP_TASK_MAX_ATTEMTPS =
      "mapred.map.max.attempts";

  /** Number of map tasks to use for an export. */
  public static final String EXPORT_MAP_TASKS_KEY =
      "sqoop.mapreduce.export.map.tasks";

  protected ImportJobContext context;

  public HdfsOdpsImportJob(SqoopOptions opts, final ImportJobContext context) {
    super(opts, null, null, null);
    this.context = context;
  }

  public Job createJob(Configuration configuration) throws IOException {
    // Put the SqoopOptions into job if requested
    if(configuration.getBoolean(SERIALIZE_SQOOPOPTIONS, SERIALIZE_SQOOPOPTIONS_DEFAULT)) {
      putSqoopOptionsToConfiguration(options, configuration);
    }

    return new Job(configuration);
  }

  public void runImport(String tableName, String ormJarFile) throws IOException {
    String tableClassName = null;
    tableClassName =
        new TableClassName(options).getClassForTable(tableName);
    Configuration conf = options.getConf();
    loadJars(conf, ormJarFile, tableClassName);
    SqoopOptions options = context.getOptions();


    Job job = createJob(conf);
    job.getConfiguration().set("mapred.jar", ormJarFile);

    if (options.getMapreduceJobName() != null) {
      job.setJobName(options.getMapreduceJobName());
    }

    propagateOptionsToJob(job);

    try {
      configureInputFormat(job, tableName, tableClassName, null);
      configureOutputFormat(job, tableName, tableClassName);
      configureMapper(job, tableName, tableClassName);
      configureNumTasks(job);
      cacheJars(job, null);
      jobSetup(job);
      setJob(job);

      boolean success = runJob(job);
      if (!success) {
        throw new ImportException("Import job failed!");
      }

      if (options.isValidationEnabled()) {
        // TODO  validateImport
      }

    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    } catch (ImportException ie) {
      throw new IOException(ie);
    } catch (InterruptedException ire) {
      throw new IOException(ire);
    } finally {
      unloadJars();
    }

  }

  @Override
  protected boolean runJob(Job job) throws ClassNotFoundException, IOException,
      InterruptedException {

    PerfCounters perfCounters = new PerfCounters();
    perfCounters.startClock();

    boolean success = doSubmitJob(job);
    perfCounters.stopClock();

    Counters jobCounters = job.getCounters();
    // If the job has been retired, these may be unavailable.
    if (null == jobCounters) {
      displayRetiredJobNotice(LOG);
    } else {
      perfCounters.addBytes(jobCounters.getGroup("FileSystemCounters")
          .findCounter("HDFS_BYTES_READ").getValue());
      LOG.info("Transferred " + perfCounters.toString());
      long numRecords =  ConfigurationHelper.getNumMapInputRecords(job);
      LOG.info("Exported " + numRecords + " records.");
    }

    return success;
  }

  protected boolean doSubmitJob(Job job)
      throws IOException, InterruptedException, ClassNotFoundException {
    return job.waitForCompletion(true);
  }

  @Override
  protected void configureInputFormat(Job job, String tableName,
                                      String tableClassName, String splitByCol)
      throws ClassNotFoundException, IOException {
    super.configureInputFormat(job, tableName, tableClassName, splitByCol);
    FileInputFormat.addInputPath(job, getInputPath());
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
    return TextExportMapper.class;
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

    conf.setStrings(OdpsConstants.INPUT_COL_NAMES, options.getColumns());

    String odpsTableName = options.getOdpsTable();
    if (odpsTableName == null) {
      odpsTableName = tableName;
    }
    conf.set(OdpsConstants.TABLE_NAME, odpsTableName);
    conf.set(OdpsConstants.ACCESS_ID, options.getOdpsAccessID());
    conf.set(OdpsConstants.ACCESS_KEY, options.getOdpsAccessKey());
    conf.set(OdpsConstants.ENDPOINT, options.getOdpsEndPoint());


    String hubEndPoint = options.getOdpsDatahubEndPoint();
    if (hubEndPoint != null) {
      conf.set(OdpsConstants.DATAHUB_ENDPOINT, hubEndPoint);
    }

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
    conf.setInt(OdpsConstants.SHARD_NUM, options.getOdpsShardNum());
    conf.setInt(OdpsConstants.SHARD_TIMEOUT, options.getOdpsShardTimeout());
    conf.setInt(OdpsConstants.RETRY_COUNT, options.getOdpsRetryCount());
    conf.setInt(OdpsConstants.BATCH_SIZE, options.getOdpsBatchSize());

    job.getConfiguration().set(ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
  }

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
    String[] columnNames = options.getColumns();

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
        colTypeMap.put(col, OdpsType.valueOf(userType.toUpperCase()));
      } else {
        LOG.warn("Column " + col
            + " type not specified, defaulting to STRING.");
        colTypeMap.put(col, OdpsType.STRING);
      }
    }
    return colTypeMap;
  }

  @Override
  protected Class<? extends InputFormat> getInputFormatClass()
      throws ClassNotFoundException {
    Class<? extends InputFormat> configuredIF = super.getInputFormatClass();
    if (null == configuredIF) {
      return ExportInputFormat.class;
    } else {
      return configuredIF;
    }
  }

  protected Path getInputPath() throws IOException {
    Path inputPath = new Path(context.getOptions().getExportDir());
    Configuration conf = options.getConf();
    inputPath = inputPath.makeQualified(FileSystem.get(conf));
    return inputPath;
  }

  @Override
  protected int configureNumMapTasks(Job job) throws IOException {
    int numMaps = super.configureNumMapTasks(job);
    job.getConfiguration().setInt(EXPORT_MAP_TASKS_KEY, numMaps);
    return numMaps;
  }

  @Override
  protected void propagateOptionsToJob(Job job) {
    super.propagateOptionsToJob(job);
    Configuration conf = job.getConfiguration();

    // This is export job where re-trying failed mapper mostly don't make sense. By
    // default we will force MR to run only one attempt per mapper. User or connector
    // developer can override this behavior by setting SQOOP_EXPORT_MAP_TASK_MAX_ATTEMTPS:
    //
    // * Positive number - we will allow specified number of attempts
    // * Negative number - we will default to Hadoop's default number of attempts
    //
    // This is important for most connectors as they are directly committing data to
    // final table and hence re-running one mapper will lead to a misleading errors
    // of inserting duplicate rows.
    int sqoopMaxAttempts = conf.getInt(SQOOP_EXPORT_MAP_TASK_MAX_ATTEMTPS, 1);
    if (sqoopMaxAttempts > 1) {
      conf.setInt(HADOOP_MAP_TASK_MAX_ATTEMTPS, sqoopMaxAttempts);
    }
  }
}
