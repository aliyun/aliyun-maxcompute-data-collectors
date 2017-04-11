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
import com.aliyun.odps.tunnel.TableTunnel;
import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.JobBase;
import com.cloudera.sqoop.orm.TableClassName;
import com.cloudera.sqoop.util.ImportException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.sqoop.lib.FieldMapProcessor;
import org.apache.sqoop.manager.ConnManager;
import org.apache.sqoop.manager.ImportJobContext;
import org.apache.sqoop.mapreduce.AvroExportMapper;
import org.apache.sqoop.mapreduce.AvroInputFormat;
import org.apache.sqoop.mapreduce.DelegatingOutputFormat;
import org.apache.sqoop.mapreduce.ExportInputFormat;
import org.apache.sqoop.mapreduce.ExportJobBase;
import org.apache.sqoop.mapreduce.ExportJobBase.FileType;
//import org.apache.sqoop.mapreduce.ParquetExportMapper;
import org.apache.sqoop.mapreduce.SequenceFileExportMapper;
import org.apache.sqoop.mapreduce.TextExportMapper;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.apache.sqoop.odps.OdpsConstants;
import org.apache.sqoop.odps.OdpsUploadProcessor;
import org.apache.sqoop.odps.OdpsUtil;
import org.apache.sqoop.util.PerfCounters;
import org.mortbay.log.Log;
import org.apache.sqoop.mapreduce.ParquetJob;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import org.kitesdk.data.mapreduce.DatasetKeyInputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetNotFoundException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Formats;

import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Arrays;

public class HdfsOdpsImportJob extends JobBase {
  public static final String SQOOP_EXPORT_MAP_TASK_MAX_ATTEMTPS =
      "sqoop.export.mapred.map.max.attempts";

  private static final String HADOOP_MAP_TASK_MAX_ATTEMTPS =
      "mapred.map.max.attempts";

  /** Number of map tasks to use for an export. */
  public static final String EXPORT_MAP_TASKS_KEY =
      "sqoop.mapreduce.export.map.tasks";

  /**
   * The (inferred) type of a file or group of files.
   */
  public enum FileType {
    SEQUENCE_FILE, AVRO_DATA_FILE, HCATALOG_MANAGED_FILE, PARQUET_FILE, UNKNOWN
  }
  
  private FileType fileType;

  protected ImportJobContext context;

//  protected TableTunnel.UploadSession uploadSession;
  
  private boolean autoCreatePartition = true;
  private boolean disableDynamicPartitions = false;

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

  private boolean isDynamicPartitions(String partitionValues) {
    if (StringUtils.isEmpty(partitionValues)) {
      return false;
    }
    return partitionValues.contains("%");
  }

  public void runImport(String tableName, String ormJarFile) throws IOException {
    String tableClassName = null;
    tableClassName =
        new TableClassName(options).getClassForTable(tableName);
    Configuration conf = options.getConf();
    loadJars(conf, ormJarFile, tableClassName);
    SqoopOptions options = context.getOptions();

    if (!isDynamicPartitions(options.getOdpsPartitionValue())) {
      options.setOdpsDisableDynamicPartitions(true);
      Log.info("Detect odps static partition specified.");
    }
    this.disableDynamicPartitions = options.isOdpsDisableDynamicPartitions();

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
  protected void configureInputFormat(Job job, String tableName, String tableClassName,
      String splitByCol) throws ClassNotFoundException, IOException {
    fileType = getInputFileType();

    super.configureInputFormat(job, tableName, tableClassName, splitByCol);

    if (isHCatJob) {
      SqoopHCatUtilities.configureExportInputFormat(options, job, context.getConnManager(),
          tableName, job.getConfiguration());
      return;
    } else if (fileType == FileType.AVRO_DATA_FILE) {
      LOG.debug("Configuring for Avro export");
      configureGenericRecordExportInputFormat(job, tableName);
    } else if (fileType == FileType.PARQUET_FILE) {
      LOG.debug("Configuring for Parquet export");
      configureGenericRecordExportInputFormat(job, tableName);
      FileSystem fs = FileSystem.get(job.getConfiguration());
      String uri = "dataset:" + fs.makeQualified(getInputPath());
      Exception caughtException = null;
      try {
        DatasetKeyInputFormat.configure(job).readFrom(uri);
      } catch (DatasetNotFoundException e) {
        LOG.warn(e.getMessage(), e);
        LOG.warn("Trying to get data schema from parquet file directly");
        caughtException = e;
      }
      if (caughtException != null && caughtException instanceof DatasetNotFoundException) {
        DatasetDescriptor descriptor = getDatasetDescriptorFromParquetFile(job, fs, uri);
        Dataset dataset = Datasets.create(uri, descriptor, GenericRecord.class);
        DatasetKeyInputFormat.configure(job).readFrom(dataset);
      }
    }

    FileInputFormat.addInputPath(job, getInputPath());
  }
  
  /**
   * Resolve a database-specific type to the Java type that should contain it.
   * @param sqlType     sql type
   * @return the name of a Java type to hold the sql datatype, or null if none.
   */
  public String toJavaType(OdpsType odpsType) {
    if (odpsType == OdpsType.VARCHAR) {
      return "String";
    } else if (odpsType == OdpsType.STRING) {
      return "String";
    } else if (odpsType == OdpsType.CHAR) {
      return "String";
    } else if (odpsType == OdpsType.DECIMAL) {
      return "java.math.BigDecimal";
    } else if (odpsType == OdpsType.BOOLEAN) {
      return "Boolean";
    } else if (odpsType == OdpsType.TINYINT) {
      return "Integer";
    } else if (odpsType == OdpsType.SMALLINT) {
      return "Integer";
    } else if (odpsType == OdpsType.BIGINT) {
      return "Long";
    } else if (odpsType == OdpsType.FLOAT) {
      return "Double";
    } else if (odpsType == OdpsType.DOUBLE) {
      return "Double";
    } else if (odpsType == OdpsType.DATE) {
      return "java.sql.Date";
    } else {
      // TODO(aaron): Support DISTINCT, ARRAY, STRUCT, REF, JAVA_OBJECT.
      // Return null indicating database-specific manager should return a
      // java data type if it can find one for any nonstandard type.
      return null;
    }
  }

  public static final PathFilter HIDDEN_FILES_PATH_FILTER = new PathFilter() {

    @Override
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  private DatasetDescriptor getDatasetDescriptorFromParquetFile(Job job, FileSystem fs, String uri)
      throws IOException {

    ArrayList<FileStatus> files = new ArrayList<FileStatus>();
    FileStatus[] dirs;
    dirs = fs.globStatus(fs.makeQualified(getInputPath()));
    for (int i = 0; (dirs != null && i < dirs.length); i++) {
      files.addAll(Arrays.asList(fs.listStatus(dirs[i].getPath(), HIDDEN_FILES_PATH_FILTER)));
      // We only check one file, so exit the loop when we have at least
      // one.
      if (files.size() > 0) {
        break;
      }
    }

    ParquetMetadata parquetMetadata;
    try {
      parquetMetadata =
          ParquetFileReader.readFooter(job.getConfiguration(),
              fs.makeQualified(files.get(0).getPath()));
    } catch (IOException e) {
      LOG.error("Wrong file format. Please check the export file's format.", e);
      throw e;
    }
    MessageType schema = parquetMetadata.getFileMetaData().getSchema();
    Schema avroSchema = new AvroSchemaConverter().convert(schema);
    DatasetDescriptor descriptor =
        new DatasetDescriptor.Builder().schema(avroSchema).format(Formats.PARQUET)
            .compressionType(ParquetJob.getCompressionType(job.getConfiguration())).build();
    return descriptor;
  }

  private void configureGenericRecordExportInputFormat(Job job, String tableName)
      throws IOException {
    if (options.getOdpsTable() != null) {
      MapWritable columnTypes = new MapWritable();
      Map<String, OdpsType> colTypeMap = getColTypeMap();
      for (Map.Entry<String, OdpsType> e : colTypeMap.entrySet()) {
        String column = e.getKey();
        if (column != null) {
          Text columnName = new Text(column);
          Text columnType = new Text(toJavaType(e.getValue()));
          columnTypes.put(columnName, columnType);
        }
      }
      DefaultStringifier.store(job.getConfiguration(), columnTypes,
          AvroExportMapper.AVRO_COLUMN_TYPES_MAP);
      return;
    }
    ConnManager connManager = context.getConnManager();
    Map<String, Integer> columnTypeInts;
    if (options.getCall() == null) {
      columnTypeInts = connManager.getColumnTypes(
          tableName,
          options.getSqlQuery());
    } else {
      columnTypeInts = connManager.getColumnTypesForProcedure(
          options.getCall());
    }
    String[] specifiedColumns = options.getColumns();
    MapWritable columnTypes = new MapWritable();
    for (Map.Entry<String, Integer> e : columnTypeInts.entrySet()) {
      String column = e.getKey();
      column = (specifiedColumns == null) ? column : options.getColumnNameCaseInsensitive(column);
      if (column != null) {
        Text columnName = new Text(column);
        Text columnType = new Text(connManager.toJavaType(tableName, column, e.getValue()));
        columnTypes.put(columnName, columnType);
      }
    }
    DefaultStringifier.store(job.getConfiguration(), columnTypes,
        AvroExportMapper.AVRO_COLUMN_TYPES_MAP);
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
    if (isHCatJob) {
      return SqoopHCatUtilities.getExportOdpsMapperClass();
    }
    switch (fileType) {
      case SEQUENCE_FILE:
        return SequenceFileExportMapper.class;
      case AVRO_DATA_FILE:
        return AvroExportMapper.class;
      case PARQUET_FILE:
        return ParquetExportMapper.class;
      case UNKNOWN:
      default:
        return TextExportMapper.class;
    }
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

    job.getConfiguration().set(ExportJobBase.SQOOP_EXPORT_TABLE_CLASS_KEY, tableClassName);
  }

  protected void jobSetup(Job job) throws IOException, ImportException {
    boolean isCreateTable = options.isOdpsCreateTable();

    String tableName =
        Preconditions.checkNotNull(options.getOdpsTable(),
            "Import to ODPS error: Table name not specified");
    String accessID =
        Preconditions
            .checkNotNull(options.getOdpsAccessID(), "Error: ODPS access ID not specified");
    String accessKey =
        Preconditions.checkNotNull(options.getOdpsAccessKey(),
            "Error: ODPS access key not specified");
    String project =
        Preconditions.checkNotNull(options.getOdpsProject(), "Error: ODPS project not specified");
    String endpoint =
        Preconditions.checkNotNull(options.getOdpsEndPoint(), "Error: ODPS endpoint not specified");

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

    if(!existsTable) {
      Map<String, OdpsType> colTypeMap = getColTypeMap();
      if (!isCreateTable) {
        LOG.warn("Could not find ODPS table " + tableName);
        LOG.warn("This job may fail. Either explicitly create the table,");
        LOG.warn("or re-run with --odps-create-table.");
      } else {
        TableSchema schema = new TableSchema();
        for (Map.Entry<String, OdpsType> colTypeEntry : colTypeMap.entrySet()) {
          schema.addColumn(new Column(colTypeEntry.getKey(), colTypeEntry.getValue()));
        }
        String partitionColumns = options.getOdpsPartitionKey();
        if (StringUtils.isNotEmpty(partitionColumns)) {
          String[] partitionCols = partitionColumns.split(",");
          for (int i = 0; i < partitionCols.length; i++) {
            schema.addPartitionColumn(new Column(partitionCols[i].trim(), OdpsType.STRING));
          }
        }
        try {
          tables.create(tableName, schema);
        } catch (OdpsException e) {
          throw new ImportException("Create table failed", e);
        }
      }
    } else {
    }

    if (options.isOdpsDisableDynamicPartitions()) {
      Table odpsTable = odps.tables().get(tableName);
      String partitionSpec =
          getPartitionSpec(odpsTable, strToArray(options.getOdpsPartitionKey()),
              strToArray(options.getOdpsPartitionValue()), Maps.newHashMap());
      if (options.isOverwriteOdpsTable()) {
        try {
          if (!odpsTable.isPartitioned()) {
            odpsTable.truncate();
          } else {
            odpsTable.deletePartition(new PartitionSpec(partitionSpec), true);
          }
        } catch (Exception e) {
          throw new RuntimeException(String.format("Truncate:%s failed.", tableName), e);
        }
      }

      if (autoCreatePartition) {
        try {
          if (odpsTable.isPartitioned()) {
            odpsTable.createPartition(new PartitionSpec(partitionSpec), true);
          }
        } catch (OdpsException e) {
          throw new RuntimeException("Create partition failed. ", e);
        }
      }

    }
  }
  
  private Map buildPartitionMap(Table odpsTable) {
    Map partMap = Maps.newHashMap();
    for (Partition partition : odpsTable.getPartitions()) {
      partMap.put(partition.getPartitionSpec().toString(), true);
//      TableSchema schema = tables.get(tableName).getSchema();
//      for(Map.Entry<String, OdpsType> colTypeEntry: colTypeMap.entrySet()) {
//        Column column = schema.getColumn(colTypeEntry.getKey().toLowerCase());
//        if (column.getType() != colTypeEntry.getValue()) {
//          throw new ImportException("Column type of ODPS table not"
//              + " consistent with user specification. Column name: "
//              + colTypeEntry.getKey());
//        }
//      }
    }
    return partMap;
  }
  
  private String[] strToArray(String s) {
    if (s == null) {
      return null;
    }
    return s.split(",");
  }
  
  private String getPartitionSpec(Table odpsTable, String[] partKeys, String[] partValues,
      Map partitionMap) {
    if (partKeys == null || partValues == null) {
      return null;
    }
    if (partKeys.length != partValues.length) {
      throw new RuntimeException("Numbers of partition key and " + "partition value are not equal.");
    }
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (int i = 0; i < partKeys.length; i++) {
      String realPartVal = partValues[i];
      sb.append(sep).append(partKeys[i]).append("='").append(realPartVal).append("'");
      sep = ",";
    }
    String partitionSpec = sb.toString();
    return partitionSpec;
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
    if (isHCatJob) {
      return SqoopHCatUtilities.getInputFormatClass();
    }
    switch (fileType) {
      case AVRO_DATA_FILE:
        return AvroInputFormat.class;
      case PARQUET_FILE:
        return DatasetKeyInputFormat.class;
      default:
        Class<? extends InputFormat> configuredIF = super.getInputFormatClass();
        if (null == configuredIF) {
          return ExportInputFormat.class;
        } else {
          return configuredIF;
        }
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
    System.out.println("sqoopMaxAttempts:" + sqoopMaxAttempts);
    if (sqoopMaxAttempts > 1) {
      conf.setInt(HADOOP_MAP_TASK_MAX_ATTEMTPS, sqoopMaxAttempts);
      conf.setInt("mapreduce.map.maxattempts", sqoopMaxAttempts);
    }

    conf.setBoolean(OdpsConstants.ODPS_DISABLE_DYNAMIC_PARTITIONS, this.disableDynamicPartitions);
//    conf.set(OdpsConstants.ODPS_TUNNEL_UPLOAD_SESSION_ID, uploadSession.getId());
  }
  
  /**
   * @return true if p is a SequenceFile, or a directory containing
   * SequenceFiles.
   */
  public static boolean isSequenceFiles(Configuration conf, Path p)
      throws IOException {
    return getFileType(conf, p) == FileType.SEQUENCE_FILE;
  }

  /**
   * @return the type of the file represented by p (or the files in p, if a
   * directory)
   */
  public static FileType getFileType(Configuration conf, Path p)
      throws IOException {
    FileSystem fs = p.getFileSystem(conf);

    try {
      FileStatus stat = fs.getFileStatus(p);

      if (null == stat) {
        // Couldn't get the item.
        LOG.warn("Input path " + p + " does not exist");
        return FileType.UNKNOWN;
      }

      if (stat.isDir()) {
        FileStatus [] subitems = fs.listStatus(p);
        if (subitems == null || subitems.length == 0) {
          LOG.warn("Input path " + p + " contains no files");
          return FileType.UNKNOWN; // empty dir.
        }

        // Pick a child entry to examine instead.
        boolean foundChild = false;
        for (int i = 0; i < subitems.length; i++) {
          stat = subitems[i];
          if (!stat.isDir() && !stat.getPath().getName().startsWith("_")) {
            foundChild = true;
            break; // This item is a visible file. Check it.
          }
        }

        if (!foundChild) {
          stat = null; // Couldn't find a reasonable candidate.
        }
      }

      if (null == stat) {
        LOG.warn("null FileStatus object in isSequenceFiles(); "
            + "assuming false.");
        return FileType.UNKNOWN;
      }

      Path target = stat.getPath();
      return fromMagicNumber(target, conf);
    } catch (FileNotFoundException fnfe) {
      LOG.warn("Input path " + p + " does not exist");
      return FileType.UNKNOWN; // doesn't exist!
    }
  }

  /**
   * @param file a file to test.
   * @return true if 'file' refers to a SequenceFile.
   */
  private static FileType fromMagicNumber(Path file, Configuration conf) {
    // Test target's header to see if it contains magic numbers indicating its
    // file type
    byte [] header = new byte[3];
    FSDataInputStream is = null;
    try {
      FileSystem fs = file.getFileSystem(conf);
      is = fs.open(file);
      is.readFully(header);
    } catch (IOException ioe) {
      // Error reading header or EOF; assume unknown
      LOG.warn("IOException checking input file header: " + ioe);
      return FileType.UNKNOWN;
    } finally {
      try {
        if (null != is) {
          is.close();
        }
      } catch (IOException ioe) {
        // ignore; closing.
        LOG.warn("IOException closing input stream: " + ioe + "; ignoring.");
      }
    }

    if (header[0] == 'S' && header[1] == 'E' && header[2] == 'Q') {
      return FileType.SEQUENCE_FILE;
    }
    if (header[0] == 'O' && header[1] == 'b' && header[2] == 'j') {
      return FileType.AVRO_DATA_FILE;
    }
    if (header[0] == 'P' && header[1] == 'A' && header[2] == 'R') {
      return FileType.PARQUET_FILE;
    }
    return FileType.UNKNOWN;
  }

  protected FileType getInputFileType() {
    if (isHCatJob) {
      return FileType.HCATALOG_MANAGED_FILE;
    }
    try {
      return getFileType(context.getOptions().getConf(), getInputPath());
    } catch (IOException ioe) {
      return FileType.UNKNOWN;
    }
  }
}
