package com.aliyun.odps.datacarrier.taskscheduler;

import org.junit.Assert;
import org.junit.Test;

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
               + "bar='hello_world'\n";
    actual = HiveSqlUtils.getUdtfSql(partitioned);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetVerifySql() throws Exception {
    // Test non partitioned table
    MetaSource.TableMetaModel nonPartitioned =
        metaSource.getTableMeta(DEFAULT_DB, "test_non_partitioned");
    String expected = "SELECT COUNT(1) FROM\n"
                      + "test.`test_non_partitioned`\n";
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
