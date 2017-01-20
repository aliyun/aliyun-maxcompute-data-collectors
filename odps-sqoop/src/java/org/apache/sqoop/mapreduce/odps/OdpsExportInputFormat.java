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

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.odps.OdpsConstants;
import org.apache.sqoop.odps.OdpsSqoopRecordReader;
import org.apache.sqoop.odps.OdpsUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class OdpsExportInputFormat extends InputFormat {
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
      Configuration conf = jobContext.getConfiguration();
      String project = conf.get(OdpsConstants.PROJECT);
      String endpoint = conf.get(OdpsConstants.ENDPOINT);
      String tableName = conf.get(OdpsConstants.TABLE_NAME);
      String partitionSpecString = conf.get(OdpsConstants.PARTITION_SPEC);

      Odps odps = new Odps(new AliyunAccount(conf.get(OdpsConstants.ACCESS_ID), conf.get(OdpsConstants.ACCESS_KEY)));
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
          long count = downloadSession.getRecordCount();

          OdpsSplitter splitter = new OdpsSplitter();
          return splitter.split(conf, count);
      } catch (TunnelException e) {
          throw new IOException(e);
      } catch (SQLException e) {
          throw new IOException(e);
      }
  }

  @Override
  public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      OdpsExportInputSplit odpsSplit = (OdpsExportInputSplit) inputSplit;
      return new OdpsSqoopRecordReader(odpsSplit);
  }

  public static class OdpsExportInputSplit extends InputSplit implements Writable {

      private long start;
      private long length;

      public OdpsExportInputSplit() {

      }

      public OdpsExportInputSplit(long start, long readCount) {
          this.start = start;
          this.length = readCount;
      }

      @Override
      public long getLength() throws IOException, InterruptedException {
          return length;
      }

      public long getStart() {
          return start;
      }

      @Override
      public String[] getLocations() throws IOException, InterruptedException {
          return new String[0];
      }

      @Override
      public void write(DataOutput dataOutput) throws IOException {
          dataOutput.writeLong(start);
          dataOutput.writeLong(start + length - 1);
      }

      @Override
      public void readFields(DataInput dataInput) throws IOException {
          start = dataInput.readLong();
          long end = dataInput.readLong();
          length = end - start + 1;
      }
  }
}
