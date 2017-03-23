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
package org.apache.sqoop.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.cloudera.sqoop.lib.FieldMapProcessor;
import com.cloudera.sqoop.lib.FieldMappable;
import com.cloudera.sqoop.lib.ProcessingException;
import com.google.common.collect.Maps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang.StringUtils;

import java.io.Closeable;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Tian Li on 15/9/29.
 */
public class OdpsUploadProcessor implements Closeable, Configurable,
        FieldMapProcessor {

  public static final Log LOG
          = LogFactory.getLog(OdpsUploadProcessor.class.getName());

  private Configuration conf;
  private Table odpsTable;
  private Odps odps;
  private OdpsRecordBuilder odpsRecordBuilder;
  private OdpsWriter odpsWriter;
  private List<OdpsRowDO> rowDOList;
  private int shardNumber;
  private int shardTimeout;
  private int retryCount;
  private int batchSize;
  private String[] partitionKeys;
  private String[] partitionValues;
  private String inputDateFormat;
  private boolean autoCreatePartition = true;
  private Map partitionMap;

  @Override
  public void close() throws IOException {
    try {
      sendBatch(rowDOList);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    try {
      odpsWriter.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    rowDOList = new LinkedList<OdpsRowDO>();

    inputDateFormat = conf.get(OdpsConstants.DATE_FORMAT);
    shardNumber = conf.getInt(OdpsConstants.SHARD_NUM,
            OdpsConstants.DEFAULT_SHARD_NUM);
    shardTimeout = conf.getInt(OdpsConstants.SHARD_TIMEOUT,
            OdpsConstants.DEFAULT_SHARD_TIMEOUT);
    retryCount = conf.getInt(OdpsConstants.RETRY_COUNT,
            OdpsConstants.DEFAULT_RETRY_COUNT);
    batchSize = conf.getInt(OdpsConstants.BATCH_SIZE,
            OdpsConstants.DEFAULT_BATCH_SIZE);

    String project = conf.get(OdpsConstants.PROJECT);
    String endpoint = conf.get(OdpsConstants.ENDPOINT);
    String tableName = conf.get(OdpsConstants.TABLE_NAME);
    String datahubEndPoint = conf.get(OdpsConstants.DATAHUB_ENDPOINT);
    String tunnelEndPoint = conf.get(OdpsConstants.TUNNEL_ENDPOINT);

    odps = new Odps(new AliyunAccount(conf.get(OdpsConstants.ACCESS_ID),
            conf.get(OdpsConstants.ACCESS_KEY)));
    odps.setUserAgent(OdpsUtil.getUserAgent());
    odpsTable = buildOdpsTable(odps, project, endpoint, tableName);

    partitionKeys = strToArray(conf.get(OdpsConstants.PARTITION_KEY));
    partitionValues = strToArray(conf.get(OdpsConstants.PARTITION_VALUE));
    if (partitionKeys != null) {
      partitionMap = buildPartitionMap();
    }

    List<String> inputColumnNames = Arrays.asList(
            conf.getStrings(OdpsConstants.INPUT_COL_NAMES));
    odpsRecordBuilder = new OdpsRecordBuilder(odpsTable,
            inputDateFormat, inputColumnNames);
    try {
      if (datahubEndPoint != null) {
        odpsWriter = buildHubWriter(project, tableName,
                datahubEndPoint, retryCount);
      } else {
        if (conf.getBoolean(OdpsConstants.ODPS_DISABLE_DYNAMIC_PARTITIONS, false)) {
          String partition = getPartitionSpec(partitionKeys, partitionValues, Maps.newHashMap());
          TableTunnel.UploadSession uploadSession = null;
          TableTunnel tunnel = new TableTunnel(odps);
              if (partition == null) {
                uploadSession = tunnel.createUploadSession(project, tableName);
              } else {
                uploadSession = tunnel.createUploadSession(project, tableName,
                    new PartitionSpec(partition));
              }
              odpsWriter = buildTunnelWriter(project, tableName, tunnelEndPoint, retryCount, uploadSession);
        } else {
          odpsWriter = buildTunnelWriter(project, tableName, tunnelEndPoint, retryCount, new String(""));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map buildPartitionMap() {
    Map partMap = Maps.newHashMap();
    for (Partition partition : odpsTable.getPartitions()) {
      partMap.put(partition.getPartitionSpec().toString(), true);
    }
    return partMap;
  }

  private String[] strToArray(String s) {
    if (s == null) {
      return null;
    }
    return s.split(",");
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void accept(FieldMappable record) throws IOException,
          ProcessingException {
    Map<String, Object> fields = record.getFieldMap();
    OdpsRowDO rowDO = new OdpsRowDO();
    try {
      rowDO.setRecord(odpsRecordBuilder.buildRecord(fields));
      String partitionSpec = getPartitionSpec(partitionKeys,
              partitionValues, fields);
      rowDO.setPartitionSpec(partitionSpec);
      rowDOList.add(rowDO);
      if (rowDOList.size() >= batchSize) {
        sendBatch(rowDOList);
      }
    } catch (Exception e) {
      throw new ProcessingException(e);
    }
  }

  private String getPartitionSpec(String[] partKeys,
                                  String[] partValues, Map fields) {
    if (partKeys == null || partValues == null) {
      return null;
    }
    if (partKeys.length != partValues.length) {
      throw new RuntimeException("Numbers of partition key and "
              + "partition value are not equal.");
    }
    StringBuilder sb = new StringBuilder();
    String sep = "";
    for (int i = 0; i < partKeys.length; i++) {
      String realPartVal = escapeString(partValues[i], fields);
      sb.append(sep).append(partKeys[i]).append("='").
              append(realPartVal).append("'");
      sep = ",";
    }
    String partitionSpec = sb.toString();
    if (autoCreatePartition && !partitionMap.containsKey(partitionSpec)) {
      try {
        odpsTable.createPartition(new PartitionSpec(partitionSpec), true);
      } catch (OdpsException e) {
        throw new RuntimeException("Create partition failed. ", e);
      }
      partitionMap.put(partitionSpec, true);
    }
    return partitionSpec;
  }

  public final static String TAG_REGEX = "\\%(\\w|\\%)|\\%\\{([\\w\\.-]+)\\}";
  public final static Pattern tagPattern = Pattern.compile(TAG_REGEX);
  public static String escapeString(String in, Map rowMap) {
    Matcher matcher = tagPattern.matcher(in);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String replacement = "";
      if(matcher.group(2) != null) {
        replacement = rowMap.get(matcher.group(2).toLowerCase()).toString();
        if(replacement == null) {
          replacement = "";
        }
      }
      replacement = replacement.replaceAll("\\\\", "\\\\\\\\");
      replacement = replacement.replaceAll("\\$", "\\\\\\$");

      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private void sendBatch(List<OdpsRowDO> rowDOList)
          throws InterruptedException, ParseException,
          TunnelException, IOException {
    if (rowDOList != null && rowDOList.size() > 0) {
      odpsWriter.write(rowDOList);
      rowDOList.clear();
    }
  }


  private Table buildOdpsTable(Odps odps, String project, String endPoint,
                               String tableName) {
    odps.setDefaultProject(project);
    odps.setEndpoint(endPoint);
    return odps.tables().get(tableName);
  }


  private OdpsWriter buildHubWriter(String project, String tableName,
                                    String datahubEndPoint, int retryCount)
          throws TunnelException, IOException, TimeoutException {
    TableTunnel tunnel = new TableTunnel(odps);
    tunnel.setEndpoint(datahubEndPoint);
    StreamClient streamClient = tunnel.createStreamClient(project, tableName);
    streamClient.loadShard(shardNumber);
    StreamWriter[] streamWriters = buildStreamWriters(streamClient);
    return new OdpsHubWriter(odpsTable, streamWriters, retryCount);
  }

  private OdpsWriter buildTunnelWriter(String project, String tableName,
                                       String tunnelEndPoint, int retryCount, String sessionId) {
    TableTunnel tunnel = new TableTunnel(odps);
    if (StringUtils.isNotEmpty(tunnelEndPoint)) {
      tunnel.setEndpoint(tunnelEndPoint);
    }
    return new OdpsTunnelWriter(tunnel, project, tableName, retryCount, sessionId);
  }
  
  private OdpsWriter buildTunnelWriter(String project, String tableName,
      String tunnelEndPoint, int retryCount, TableTunnel.UploadSession uploadSession) throws TunnelException {
    TableTunnel tunnel = new TableTunnel(odps);
    if (StringUtils.isNotEmpty(tunnelEndPoint)) {
      tunnel.setEndpoint(tunnelEndPoint);
    }
    return new OdpsTunnelWriter(tunnel, project, tableName, retryCount, uploadSession);
}

  private StreamWriter[] buildStreamWriters(StreamClient streamClient)
          throws IOException, TunnelException,
          TimeoutException {
    StreamWriter[] streamWriters;
    final StreamClient.ShardState finish = StreamClient.ShardState.LOADED;
    long now = System.currentTimeMillis();
    long endTime = now + shardTimeout * 1000;
    List<Long> shardIDList = null;
    while (now < endTime) {
      HashMap<Long, StreamClient.ShardState> shardStatus = streamClient
              .getShardStatus();
      shardIDList = new ArrayList<Long>();
      Set<Long> keys = shardStatus.keySet();
      Iterator<Long> iter = keys.iterator();
      while (iter.hasNext()) {
        Long key = iter.next();
        StreamClient.ShardState value = shardStatus.get(key);
        if (value.equals(finish)) {
          shardIDList.add(key);
        }
      }
      now = System.currentTimeMillis();
      if (shardIDList.size() == shardNumber) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // DO NOTHING...
      }
    }
    if (shardIDList != null && shardIDList.size() > 0) {
      streamWriters = new StreamWriter[shardIDList.size()];
      for (int i = 0; i < shardIDList.size(); i++) {
        streamWriters[i] = streamClient.openStreamWriter(shardIDList.get(i));
      }
    } else {
      throw new TimeoutException("buildStreamWriters() error, " +
              "have no loaded shards.");
    }
    return streamWriters;
  }
}
