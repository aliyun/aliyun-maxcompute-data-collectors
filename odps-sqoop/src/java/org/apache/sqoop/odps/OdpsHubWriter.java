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

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamRecordPack;
import com.aliyun.odps.tunnel.io.StreamWriter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by Tian Li on 15/9/29.
 */
public class OdpsHubWriter extends OdpsWriter {
  public static final Log LOG = LogFactory.getLog(OdpsHubWriter.class.getName());
  private StreamWriter[] streamWriters;
  private Random random;
  private TableSchema tableSchema;
  private int retryCount;

  public OdpsHubWriter(Table odpsTable, StreamWriter[] streamWriters,
                       int retryCount) {
    this.streamWriters = streamWriters;
    tableSchema = odpsTable.getSchema();
    this.random = new Random();
    this.retryCount = retryCount;
  }

  public void write(List<OdpsRowDO> rowList) throws InterruptedException,
          TunnelException, IOException {
    if (rowList == null || rowList.isEmpty()) {
      return;
    }
    List<OdpsStreamRecordPackDO> packDOList = buildRecordPackList(rowList);
    if (packDOList == null || packDOList.isEmpty()) {
      return;
    }
    for (OdpsStreamRecordPackDO streamRecordPackDO : packDOList) {
      int retry = 0;
      while (true) {
        try {
          writePack(streamRecordPackDO);
          break;
        } catch (Exception e) {
          LOG.warn("Upload exception in retry " + retry, e);
          retry++;
          if (retry > retryCount) {
            throw new RuntimeException("Retry failed. " +
                    "Retry count reaches limit.", e);
          }
          int sleepTime = 50 + 1000 * (retry - 1);
          Thread.sleep(sleepTime);
        }
      }
    }
  }

  private void writePack(OdpsStreamRecordPackDO packDO) throws IOException,
          TunnelException {
    if (StringUtils.isEmpty(packDO.getPartitionSpec())) {
      streamWriters[random.nextInt(streamWriters.length)]
              .write(packDO.getRecordPack());
    } else {
      streamWriters[random.nextInt(streamWriters.length)]
              .write(new PartitionSpec(packDO.getPartitionSpec()),
                      packDO.getRecordPack());
    }
  }

  private List<OdpsStreamRecordPackDO> buildRecordPackList(
          List<OdpsRowDO> rowDOList) throws IOException {
    if (rowDOList == null || rowDOList.isEmpty()) {
      return null;
    }
    List<OdpsStreamRecordPackDO> recordPackDOList = Lists.newArrayList();
    Map<String, OdpsStreamRecordPackDO> partitionPackMap = Maps.newHashMap();
    for (OdpsRowDO rowDO : rowDOList) {
      OdpsStreamRecordPackDO packDO
              = partitionPackMap.get(rowDO.getPartitionSpec());
      if (packDO == null) {
        packDO = new OdpsStreamRecordPackDO();
        StreamRecordPack streamRecordPack = new StreamRecordPack(tableSchema);
        packDO.setPartitionSpec(rowDO.getPartitionSpec());
        packDO.setRecordPack(streamRecordPack);
        partitionPackMap.put(rowDO.getPartitionSpec(), packDO);
      }
      packDO.getRecordPack().append(rowDO.getRecord());
    }
    if (partitionPackMap.keySet().size() > 0) {
      recordPackDOList.addAll(partitionPackMap.values());
    }
    return recordPackDOList;

  }
}
