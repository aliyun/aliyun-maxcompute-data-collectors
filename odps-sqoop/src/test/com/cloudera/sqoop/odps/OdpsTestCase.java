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
package com.cloudera.sqoop.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.cloudera.sqoop.testutil.HsqldbTestServer;
import com.cloudera.sqoop.testutil.ImportJobTestCase;
import org.junit.Before;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public abstract class OdpsTestCase extends ImportJobTestCase {

  static final String PROJECT
          = System.getProperty("sqoop.test.odps.project");
  static final String ACCESS_ID
          = System.getProperty("sqoop.test.odps.accessid");
  static final String ACCESS_KEY
          = System.getProperty("sqoop.test.odps.accesskey");
  static final String ENDPOINT
          = System.getProperty("sqoop.test.odps.endpoint",
          "http://service.odps.aliyun.com/api");
  static final String TUNNEL_ENDPOINT
          = System.getProperty("sqoop.test.odps.tunnel_endpoint",
          "http://dh.odps.aliyun.com");
  static final String DATAHUB_ENDPOINT
          = System.getProperty("sqoop.test.odps.datahub_endpoint");

  protected String [] getArgv(String odpsTable, String queryStr,
                              boolean useHub, boolean isCreate) {
    ArrayList<String> args = new ArrayList<String>();
    if (null != queryStr) {
      args.add("--query");
      args.add(queryStr);
    } else {
      args.add("--table");
      args.add(getTableName());
    }
    args.add("--split-by");
    args.add(getColName(0));
    args.add("--connect");
    args.add(HsqldbTestServer.getUrl());
    args.add("--num-mappers");
    args.add("1");
    args.add("--odps-table");
    args.add(odpsTable);
    args.add("--odps-project");
    args.add(PROJECT);
    args.add("--odps-accessid");
    args.add(ACCESS_ID);
    args.add("--odps-accesskey");
    args.add(ACCESS_KEY);
    args.add("--odps-endpoint");
    args.add(ENDPOINT);
    if (useHub) {
      args.add("--odps-datahub-endpoint");
      args.add(DATAHUB_ENDPOINT);
    } else {
      args.add("--odps-tunnel-endpoint");
      args.add(TUNNEL_ENDPOINT);
    }
    if (isCreate) {
      args.add("--create-odps-table");
    }

    return args.toArray(new String[0]);
  }

  private Odps odps;

  @Override
  @Before
  public void setUp() {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    odps = new Odps(account);
    odps.setDefaultProject(PROJECT);
    odps.setEndpoint(ENDPOINT);
    super.setUp();
  }

  protected void verifyOdpsDataset(String tableName, String partitionSpec,
                                   Object[][] valsArray)
      throws OdpsException, IOException {
    TableTunnel tunnel = new TableTunnel(odps);
    TableTunnel.DownloadSession downloadSession;
    if (partitionSpec != null) {
      downloadSession = tunnel.createDownloadSession(PROJECT, tableName,
          new PartitionSpec(partitionSpec));
    } else {
      downloadSession = tunnel.createDownloadSession(PROJECT, tableName);
    }
    long count = downloadSession.getRecordCount();
    RecordReader recordReader = downloadSession.openRecordReader(0, count);
    try {
      List<String> expectations = new ArrayList<String>();
      if (valsArray != null) {
        for (Object[] vals: valsArray) {
          expectations.add(Arrays.toString(vals));
        }
      }
      Record record;
      while ((record = recordReader.read()) != null
          && expectations.size() > 0) {
        String actual = Arrays.toString(convertOdpsRecordToArray(record));
        assertTrue("Expect record: " + actual, expectations.remove(actual));
      }
      assertFalse(record != null);
      assertEquals(0, expectations.size());
    } finally {
      recordReader.close();
    }
  }

  private static Object[] convertOdpsRecordToArray(Record record) {
    Column[] columns = record.getColumns();
    Object[] result = new Object[columns.length];
    for (int i = 0; i < result.length; i++) {
      String colValue;
      switch (columns[i].getType()) {
        case BIGINT: {
          Long v = record.getBigint(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case BOOLEAN: {
          Boolean v = record.getBoolean(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DATETIME: {
          Date v = record.getDatetime(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DOUBLE: {
          Double v = record.getDouble(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case STRING: {
          String v = record.getString(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        case DECIMAL: {
          BigDecimal v = record.getDecimal(i);
          colValue = v == null ? null : v.toString();
          break;
        }
        default:
          throw new RuntimeException("Unknown column type: "
              + columns[i].getType());
      }
      result[i] = colValue;
    }
    return result;
  }

  protected boolean isTableExist(String tableName) throws OdpsException {
    return odps.tables().exists(tableName);
  }

  protected void deleteTable(String tableName) throws OdpsException {
    odps.tables().delete(tableName, true);
  }
}
