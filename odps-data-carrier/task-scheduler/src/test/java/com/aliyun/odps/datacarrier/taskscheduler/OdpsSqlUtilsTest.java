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

// TODO: use interfaces provided by odps sdk to create table and partitions
public class OdpsSqlUtilsTest {

  private static final String DEFAULT_DB = "test";

  private static MetaSource metaSource = new MockHiveMetaSource();

  @Test
  public void testGetCreateTableStatement() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    String expected = "CREATE TABLE IF NOT EXISTS test.`test_non_partitioned` (\n"
                      + "    `foo` string\n"
                      + ");\n";
    String actual = OdpsSqlUtils.getCreateTableStatement(nonPartitioned);
    Assert.assertEquals(expected, actual);

    // Test partitioned table
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned");
    expected = "CREATE TABLE IF NOT EXISTS test.`test_partitioned` (\n"
               + "    `foo` string\n"
               + ")\n"
               + "PARTITIONED BY (\n"
               + "    `bar` string\n"
               + ");\n";
    actual = OdpsSqlUtils.getCreateTableStatement(partitioned);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetAddPartitionStatements() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    // Expect an IllegalArgumentException with non partitioned table
    try {
      OdpsSqlUtils.getAddPartitionStatement(nonPartitioned);
      Assert.fail("Expect an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Not a partitioned table", e.getMessage());
    }

    // Test partitioned table
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned");
    String expected = "ALTER TABLE\n"
                      + "test.`test_partitioned`\n"
                      + "ADD IF NOT EXISTS\n"
                      + "PARTITION (bar='hello_world');\n";
    String actual = OdpsSqlUtils.getAddPartitionStatement(partitioned);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetAddPartitionStatementsNoPartition() throws Exception {
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned").clone();
    partitioned.partitions = new LinkedList<>();

    String actual = OdpsSqlUtils.getAddPartitionStatement(partitioned);
    Assert.assertEquals("", actual);
  }

  @Test
  public void testGetDropPartitionStatementsNoPartition() throws Exception {
    MetaSource.TableMetaModel partitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_partitioned").clone();
    partitioned.partitions = new LinkedList<>();

    String actual = OdpsSqlUtils.getDropPartitionStatement(partitioned);
    Assert.assertEquals("", actual);
  }

  @Test
  public void testGetVerifySql() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    String expected = "SELECT COUNT(1) FROM\n"
                      + "test.`test_non_partitioned`\n"
                      + ";\n";
    String actual = OdpsSqlUtils.getVerifySql(nonPartitioned);
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
