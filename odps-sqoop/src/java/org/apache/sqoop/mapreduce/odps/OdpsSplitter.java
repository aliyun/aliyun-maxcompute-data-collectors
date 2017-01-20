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

import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.cloudera.sqoop.config.ConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.sqoop.mapreduce.db.IntegerSplitter;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OdpsSplitter extends IntegerSplitter {
  public List<InputSplit> split(Configuration conf, long count) 
      throws SQLException, IOException, TunnelException {

    int numSplits = ConfigurationHelper.getConfNumMaps(conf);
    if (numSplits < 1) {
        numSplits = 1;
    }
    long splitLimit = org.apache.sqoop.config.ConfigurationHelper
        .getSplitLimit(conf);
    List<Long> splitPoints = split(numSplits, splitLimit, 0, count);
    List<InputSplit> splits = new ArrayList<InputSplit>();

    long start = splitPoints.get(0);
    for (int i = 1; i < splitPoints.size(); i++) {
      long end = splitPoints.get(i);
      long readLength;
      if (i == splitPoints.size() - 1) {
          readLength = end - start;
      } else {
          readLength = end - start - 1;
      }
      splits.add(new OdpsExportInputFormat
          .OdpsExportInputSplit(start, readLength));
      start = end;
    }

    return splits;
  }
}
