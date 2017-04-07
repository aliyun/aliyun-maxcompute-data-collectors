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
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class OdpsTunnelWriter extends OdpsWriter {
  public static final Log LOG = LogFactory.getLog(OdpsTunnelWriter.class.getName());
  private TableTunnel tunnel;
  private String project;
  private String tableName;
  private int retryCount;
  private String sharedSessionId;
  private TableTunnel.UploadSession sharedUploadSession;
  private RecordWriter sharedWriter;

  public OdpsTunnelWriter(TableTunnel tunnel, String project,
                          String tableName, int retryCount, String sessionId) {
    this.tunnel = tunnel;
    this.project = project;
    this.tableName = tableName;
    this.retryCount = retryCount;
    this.sharedSessionId = sessionId;
  }

  public OdpsTunnelWriter(TableTunnel tunnel, String project, String tableName, int retryCount,
      UploadSession uploadSession) throws TunnelException {
    this.tunnel = tunnel;
    this.project = project;
    this.tableName = tableName;
    this.retryCount = retryCount;
    this.sharedUploadSession = uploadSession;
    sharedWriter = uploadSession.openBufferedWriter(true);
    ((TunnelBufferedWriter)sharedWriter).setBufferSize(256*1024*1024);
  }

  @Override
  public void write(List<OdpsRowDO> rowList)
          throws InterruptedException, TunnelException, IOException {
    if (rowList == null || rowList.isEmpty()) {
      return;
    }
    Map<String, List<Record>> partitionRecordMap
            = new HashMap<String, List<Record>>();
    for (OdpsRowDO rowDO: rowList) {
      String partitionString = rowDO.getPartitionSpec();
      List<Record> recordList = partitionRecordMap.get(partitionString);
      if (recordList == null) {
        recordList = new LinkedList<Record>();
        recordList.add(rowDO.getRecord());
        partitionRecordMap.put(partitionString, recordList);
      } else {
        recordList.add(rowDO.getRecord());
      }
    }
    for (Map.Entry<String, List<Record>> mapEntry
            : partitionRecordMap.entrySet()) {
      int retry = 0;
      if (sharedUploadSession != null) {
        for (Record r : mapEntry.getValue()) {
          sharedWriter.write(r);
        }
        continue;
      }
      
      while (true) {
        RecordWriter writer = null;
        try {
          String partition = mapEntry.getKey();
          TableTunnel.UploadSession uploadSession;
          if (partition == null) {
            uploadSession = tunnel.createUploadSession(project, tableName);
          } else {
            uploadSession = tunnel.createUploadSession(project, tableName,
                new PartitionSpec(partition));
          }
          writer = uploadSession.openRecordWriter(0);
          for (Record r : mapEntry.getValue()) {
            writer.write(r);
          }
          writer.close();
          uploadSession.commit(new Long[]{0L});
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
        } finally {
          try {
            if (writer != null) {
              writer.close();
            }
          } catch (Exception e) {
            // Do Nothing
          }
        }
      }
    }
  }

  @Override
  public void close() throws InterruptedException, TunnelException, IOException {
    if (sharedWriter != null) {
      sharedWriter.close();
    }
    if (sharedUploadSession != null) {
      int count = 0;
      int retryTimeLimit = 6;
      while (count < retryTimeLimit) {
        try {
          sharedUploadSession.commit();
          break;
        } catch (TunnelException te) {
          LOG.warn("Commit exception in retry " + count, te);
          Thread.sleep(5*1000);
        }
        count++;
      }
      
      if (count >= retryTimeLimit) {
        throw new IOException("Upload Session commit failed, after retry " + retryTimeLimit + " times.");
      }
    }
  }
}
