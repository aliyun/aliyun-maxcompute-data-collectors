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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.cloudera.sqoop.lib.SqoopRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.sqoop.mapreduce.odps.OdpsExportInputFormat;

import java.io.IOException;
public class OdpsSqoopRecordReader extends RecordReader<LongWritable, Record> {
  private TaskAttemptContext context;

  private LongWritable key;
  private Record value;

  private com.aliyun.odps.data.RecordReader odpsRecordReader;

  private long start;
  private long length;
  private long pos;

  public OdpsSqoopRecordReader(OdpsExportInputFormat
      .OdpsExportInputSplit split)
          throws IOException, InterruptedException {
    this.start = split.getStart();
    this.length = split.getLength();
    this.pos = this.start;
  }

  @Override
  public void initialize(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) 
          throws IOException, InterruptedException {
    Configuration conf = taskAttemptContext.getConfiguration();
    String project = conf.get(OdpsConstants.PROJECT);
    String endpoint = conf.get(OdpsConstants.ENDPOINT);
    String tableName = conf.get(OdpsConstants.TABLE_NAME);
    String partitionSpecString = conf.get(OdpsConstants.PARTITION_SPEC);
    String accessID = conf.get(OdpsConstants.ACCESS_ID);
    String accessKey = conf.get(OdpsConstants.ACCESS_KEY);

    Odps odps = new Odps(new AliyunAccount(accessID, accessKey));
    odps.setUserAgent(OdpsUtil.getUserAgent());
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(project);
    TableTunnel tunnel = new TableTunnel(odps);
    PartitionSpec partitionSpec = null;
    if (partitionSpecString != null) {
      partitionSpec = new PartitionSpec(partitionSpecString);
    }
    TableTunnel.DownloadSession downloadSession;
    try {
      if (partitionSpec == null) {
        downloadSession = tunnel.createDownloadSession(project, tableName);
      } else {
        downloadSession = tunnel.createDownloadSession(project, tableName,
              partitionSpec);
      }
      this.odpsRecordReader = downloadSession.openRecordReader(start, length);
    } catch (TunnelException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (this.key == null) {
      this.key = new LongWritable();
    }
    this.key.set(this.pos);
    this.value = this.odpsRecordReader.read();
    if (this.value == null) {
      return false;
    }

    this.pos += 1;
    return true;
  }

  @Override
  public LongWritable getCurrentKey()
      throws IOException, InterruptedException {
    return this.key;
  }

  @Override
  public Record getCurrentValue() throws IOException, InterruptedException {
    return this.value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return this.length == 0 ? 0.0F 
        : Math.min(1.0F,
            (float) (this.pos - this.start) / (float) this.length);
  }

  @Override
  public void close() throws IOException {
    // DO NOTHING
  }
}
