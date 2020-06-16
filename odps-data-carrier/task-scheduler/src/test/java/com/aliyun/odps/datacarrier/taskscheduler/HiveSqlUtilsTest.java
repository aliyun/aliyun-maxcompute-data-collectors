/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;

public class HiveSqlUtilsTest {

  private static final String DEFAULT_DB = "test";

  private static MetaSource metaSource = new MockHiveMetaSource();

  @Test
  public void testGetUdtfSql() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    String expected = "SELECT odps_data_dump_multi(\n"
                      + "'test',\n"
                      + "'test_non_partitioned',\n"
                      + "'foo',\n"
                      + "'',\n"
                      + "`foo`)\n"
                      + "FROM test.`test_non_partitioned`\n";
    String actual = HiveSqlUtils.getUdtfSql(nonPartitioned);
    Assert.assertEquals(expected, actual);

    // Test partitioned table
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned");
    expected = "SELECT odps_data_dump_multi(\n"
               + "'test',\n"
               + "'test_partitioned',\n"
               + "'foo',\n"
               + "'bar',\n"
               + "`foo`,\n"
               + "`bar`)\n"
               + "FROM test.`test_partitioned`\n"
               + "WHERE\n"
               + "bar=cast('hello_world' AS string)\n";
    actual = HiveSqlUtils.getUdtfSql(partitioned);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetUdtfSqlNoPartition() throws Exception {
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned").clone();
    partitioned.partitions = new LinkedList<>();

    String expected = "SELECT odps_data_dump_multi(\n"
                      + "'test',\n"
                      + "'test_partitioned',\n"
                      + "'foo',\n"
                      + "'bar',\n"
                      + "`foo`,\n"
                      + "`bar`)\n"
                      + "FROM test.`test_partitioned`\n";
    String actual = HiveSqlUtils.getUdtfSql(partitioned);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetVerifySql() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    String expected = "SELECT COUNT(1) FROM\n"
                      + "test.`test_non_partitioned`\n\n";
    String actual = HiveSqlUtils.getVerifySql(nonPartitioned);
    Assert.assertEquals(expected, actual);

    // Test partitioned table
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned");
    expected = "SELECT `bar`, COUNT(1) FROM\n"
               + "test.`test_partitioned`\n"
               + "WHERE\n"
               + "bar='hello_world'\n"
               + "GROUP BY `bar`\n"
               + "ORDER BY `bar`\n"
               + "LIMIT 1;\n";
    actual = OdpsSqlUtils.getVerifySql(partitioned);
    Assert.assertEquals(expected, actual);
  }
}
