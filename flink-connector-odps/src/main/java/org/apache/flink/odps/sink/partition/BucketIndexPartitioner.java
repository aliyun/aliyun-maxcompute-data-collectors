/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.odps.sink.partition;

import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.hasher.OdpsHasher;
import com.aliyun.odps.tunnel.hasher.TypeHasher;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.odps.FlinkOdpsException;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;

// TODO: for bucket
public class BucketIndexPartitioner implements Partitioner<RowData> {

  private final int bucketNum;
  private final List<Integer> hashKeys;

  public BucketIndexPartitioner(int bucketNum, List<Integer> hashKeys) {
      this.bucketNum = bucketNum;
      this.hashKeys = hashKeys;
  }

  @Override
  public int partition(RowData key, int numPartitions) {
    List<Integer> hashValues = new ArrayList<>();
    for (int hashKey : hashKeys) {
      Object value = key.getLong(hashKey);
      if (value == null) {
          throw new FlinkOdpsException("Hash key " + key + " can not be null!");
      }
      OdpsHasher hasher = TypeHasher.getHasher("bigint",
                      "default");
      hashValues.add(hasher.hash(value));
    }
    int bucket = TypeHasher.CombineHashVal(hashValues) % bucketNum;
    return bucket % numPartitions;
  }
}
