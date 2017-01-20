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

/**
 * Created by Tian Li on 15/9/29.
 */
public class OdpsConstants {

  private OdpsConstants() {}

  public static final String ACCESS_ID = "sqoop.odps.access.id";
  public static final String ACCESS_KEY = "sqoop.odps.access.key";
  public static final String PROJECT = "sqoop.odps.project";
  public static final String ENDPOINT = "sqoop.odps.endpoint";
  public static final String DATAHUB_ENDPOINT = "sqoop.odps.datahub.endpoint";
  public static final String TUNNEL_ENDPOINT = "sqoop.odps.tunnel.endpoint";
  public static final String TABLE_NAME = "sqoop.odps.tablename";
  public static final String INPUT_COL_NAMES = "sqoop.odps.inputcolnames";

  public static final String PARTITION_KEY = "sqoop.odps.partition.key";
  public static final String PARTITION_VALUE = "sqoop.odps.partition.value";
  public static final String CREATE_TABLE = "sqoop.odps.create";
  public static final String DATE_FORMAT = "sqoop.odps.dateformat";
  public static final String SHARD_NUM = "sqoop.odps.shard.num";
  public static final String SHARD_TIMEOUT = "sqoop.odps.shard.timeout";
  public static final String RETRY_COUNT = "sqoop.odps.retrycount";
  public static final String BATCH_SIZE = "sqoop.odps.batchsize";

  public static final String PARTITION_SPEC = "sqoop.odps.partition.spec";

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final int DEFAULT_SHARD_NUM = 1;
  public static final int DEFAULT_SHARD_TIMEOUT = 60;
  public static final int DEFAULT_RETRY_COUNT = 3;
  public static final int DEFAULT_HUBLIFECYCLE = 7;
}
