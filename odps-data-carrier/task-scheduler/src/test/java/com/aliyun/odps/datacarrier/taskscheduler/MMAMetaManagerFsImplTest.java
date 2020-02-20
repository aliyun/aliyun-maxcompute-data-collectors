package com.aliyun.odps.datacarrier.taskscheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.aliyun.odps.datacarrier.metacarrier.MetaSource;

public class MMAMetaManagerFsImplTest {
  private static final Path DEFAULT_MMA_META_DIR =
      Paths.get(System.getProperty("user.dir"), ".mma").toAbsolutePath();

  private static final String DEFAULT_DB = "test";

  private MMAMetaManager metaManager;
  private MetaSource metaSource;

  @Before
  public void before() throws Exception {
    metaSource = new MockHiveMetaSource();
    metaManager = new MMAMetaManagerFsImpl(null, metaSource);
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      metaManager.initMigration(DEFAULT_DB, table, new TableMigrationConfig());
    }
  }

  @After
  public void after() {
    DirUtils.removeDir(DEFAULT_MMA_META_DIR);
  }

  @Test
  public void testInitTask() throws Exception {
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(DEFAULT_DB, table);

      // Make sure the metadata dir exists
      Path dir = Paths.get(DEFAULT_MMA_META_DIR.toString(), DEFAULT_DB, table);
      assertTrue(dir.toFile().exists());

      // Make sure the table metadata file exists
      Path metadataPath = Paths.get(dir.toString(), "metadata_" + table);
      assertTrue(metadataPath.toFile().exists());

      // Make sure the content of table metadata file is expected
      String metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0), metadata);

      // Make sure the config file exists
      Path configPath = Paths.get(dir.toString(), "config");
      assertTrue(configPath.toFile().exists());

      // Make sure the partition metadata file exists and its content is expected
      if (tableMetaModel.partitionColumns.size() > 0) {
        Path partitionMetadataPath = Paths.get(dir.toString(), "metadata_pt_hello_world");
        assertTrue(partitionMetadataPath.toFile().exists());

        String partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0),
                     partitionMetadata);
      }
    }
  }

  @Test
  public void testGetStatus() throws Exception {
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(DEFAULT_DB, table);

      Path dir = Paths.get(DEFAULT_MMA_META_DIR.toString(), DEFAULT_DB, table);
      Path metadataPath = Paths.get(dir.toString(), "metadata_" + table);

      // Make sure the content of table metadata file is expected
      String metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0), metadata);
      assertEquals(MMAMetaManager.MigrationStatus.PENDING,
                   metaManager.getStatus(DEFAULT_DB, table));

      // Make sure the partition metadata file exists and its content is expected
      if (tableMetaModel.partitionColumns.size() > 0) {
        Path partitionMetadataPath = Paths.get(dir.toString(), "metadata_pt_hello_world");
        String partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0),
                     partitionMetadata);

        List<String> partitionValues = tableMetaModel.partitions.get(0).partitionValues;
        assertEquals(MMAMetaManager.MigrationStatus.PENDING,
                     metaManager.getStatus(DEFAULT_DB, table, partitionValues));
      }
    }
  }

  @Test
  public void testUpdateStatus() throws Exception {
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(DEFAULT_DB, table);

      Path dir = Paths.get(DEFAULT_MMA_META_DIR.toString(), DEFAULT_DB, table);
      Path metadataPath = Paths.get(dir.toString(), "metadata_" + table);

      String metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0), metadata);
      metaManager.updateStatus(DEFAULT_DB, table, MMAMetaManager.MigrationStatus.RUNNING);
      metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.RUNNING, 0), metadata);


      // Make sure the partition metadata file exists and its content is expected
      if (tableMetaModel.partitionColumns.size() > 0) {
        Path partitionMetadataPath = Paths.get(dir.toString(), "metadata_pt_hello_world");
        String partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0),
                     partitionMetadata);

        List<String> partitionValues = tableMetaModel.partitions.get(0).partitionValues;
        List<List<String>> partitionValuesList = new LinkedList<>();
        partitionValuesList.add(partitionValues);
        metaManager.updateStatus(DEFAULT_DB, table, partitionValuesList,
                                 MMAMetaManager.MigrationStatus.RUNNING);
        partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.RUNNING, 0),
                     partitionMetadata);
      }
    }
  }

  @Test
  public void testGetFailedTimes() throws Exception {
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(DEFAULT_DB, table);

      Path dir = Paths.get(DEFAULT_MMA_META_DIR.toString(), DEFAULT_DB, table);
      Path metadataPath = Paths.get(dir.toString(), "metadata_" + table);

      String metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0), metadata);
      assertEquals(0, metaManager.getFailedTimes(DEFAULT_DB, table));


      // Make sure the partition metadata file exists and its content is expected
      if (tableMetaModel.partitionColumns.size() > 0) {
        Path partitionMetadataPath = Paths.get(dir.toString(), "metadata_pt_hello_world");
        String partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0),
                     partitionMetadata);

        List<String> partitionValues = tableMetaModel.partitions.get(0).partitionValues;
        assertEquals(0, metaManager.getFailedTimes(DEFAULT_DB, table, partitionValues));
      }
    }
  }

  @Test
  public void testIncreaseFailedTimes() throws Exception {
    for (String table : metaSource.listTables(DEFAULT_DB)) {
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(DEFAULT_DB, table);

      Path dir = Paths.get(DEFAULT_MMA_META_DIR.toString(), DEFAULT_DB, table);
      Path metadataPath = Paths.get(dir.toString(), "metadata_" + table);

      String metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0), metadata);
      metaManager.increaseFailedTimes(DEFAULT_DB, table);
      metadata = DirUtils.readFile(metadataPath);
      assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 1), metadata);


      // Make sure the partition metadata file exists and its content is expected
      if (tableMetaModel.partitionColumns.size() > 0) {
        Path partitionMetadataPath = Paths.get(dir.toString(), "metadata_pt_hello_world");
        String partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 0),
                     partitionMetadata);

        List<String> partitionValues = tableMetaModel.partitions.get(0).partitionValues;
        metaManager.increaseFailedTimes(DEFAULT_DB, table, partitionValues);
        partitionMetadata = DirUtils.readFile(partitionMetadataPath);
        assertEquals(String.format("%s\n%d", MMAMetaManager.MigrationStatus.PENDING, 1),
                     partitionMetadata);
      }
    }
  }

  @Test
  public void testGetPendingTables() {
    List<MetaSource.TableMetaModel> pendingTables = metaManager.getPendingTables();
    assertEquals(2, pendingTables.size());

    for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
      if ("test_non_partitioned".equals(tableMetaModel.tableName)) {
        assertEquals(DEFAULT_DB, tableMetaModel.odpsProjectName);
        assertEquals("test_non_partitioned", tableMetaModel.odpsTableName);
        assertEquals(1, tableMetaModel.columns.size());
        assertEquals("foo", tableMetaModel.columns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.columns.get(0).odpsType);
        assertEquals(0, tableMetaModel.partitionColumns.size());
        assertEquals(0, tableMetaModel.partitions.size());
      } else if ("test_partitioned".equals(tableMetaModel.tableName)) {
        assertEquals(DEFAULT_DB, tableMetaModel.odpsProjectName);
        assertEquals("test_partitioned", tableMetaModel.odpsTableName);
        assertEquals(1, tableMetaModel.columns.size());
        assertEquals("foo", tableMetaModel.columns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.columns.get(0).odpsType);
        assertEquals(1, tableMetaModel.partitionColumns.size());
        assertEquals("bar", tableMetaModel.partitionColumns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.partitionColumns.get(0).odpsType);
        assertEquals(1, tableMetaModel.partitions.size());
        assertEquals("hello_world", tableMetaModel.partitions.get(0).partitionValues.get(0));
      }
    }
  }

  @Test
  public void testGetPendingTablesAfterUpdateTableStatus() {
    metaManager.updateStatus(DEFAULT_DB, "test_non_partitioned",
                             MMAMetaManager.MigrationStatus.RUNNING);

    List<MetaSource.TableMetaModel> pendingTables = metaManager.getPendingTables();
    assertEquals(1, pendingTables.size());

    MetaSource.TableMetaModel tableMetaModel = pendingTables.get(0);
    assertEquals(DEFAULT_DB, tableMetaModel.odpsProjectName);
    assertEquals("test_partitioned", tableMetaModel.odpsTableName);
    assertEquals(1, tableMetaModel.columns.size());
    assertEquals("foo", tableMetaModel.columns.get(0).odpsColumnName);
    assertEquals("string", tableMetaModel.columns.get(0).odpsType);
    assertEquals(1, tableMetaModel.partitionColumns.size());
    assertEquals("bar", tableMetaModel.partitionColumns.get(0).odpsColumnName);
    assertEquals("string", tableMetaModel.partitionColumns.get(0).odpsType);
    assertEquals(1, tableMetaModel.partitions.size());
    assertEquals("hello_world", tableMetaModel.partitions.get(0).partitionValues.get(0));
  }

  @Test
  public void testGetPendingTablesAfterUpdatePartitionStatus() {
    List<String> partitionValues = new LinkedList<>();
    partitionValues.add("hello_world");
    List<List<String>> partitionValuesList = new LinkedList<>();
    partitionValuesList.add(partitionValues);
    metaManager.updateStatus(DEFAULT_DB, "test_partitioned", partitionValuesList,
                             MMAMetaManager.MigrationStatus.RUNNING);

    List<MetaSource.TableMetaModel> pendingTables = metaManager.getPendingTables();
    assertEquals(2, pendingTables.size());

    for (MetaSource.TableMetaModel tableMetaModel : pendingTables) {
      if ("test_non_partitioned".equals(tableMetaModel.tableName)) {
        assertEquals(DEFAULT_DB, tableMetaModel.odpsProjectName);
        assertEquals("test_non_partitioned", tableMetaModel.odpsTableName);
        assertEquals(1, tableMetaModel.columns.size());
        assertEquals("foo", tableMetaModel.columns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.columns.get(0).odpsType);
        assertEquals(0, tableMetaModel.partitionColumns.size());
        assertEquals(0, tableMetaModel.partitions.size());
      } else if ("test_partitioned".equals(tableMetaModel.tableName)) {
        assertEquals(DEFAULT_DB, tableMetaModel.odpsProjectName);
        assertEquals("test_partitioned", tableMetaModel.odpsTableName);
        assertEquals(1, tableMetaModel.columns.size());
        assertEquals("foo", tableMetaModel.columns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.columns.get(0).odpsType);
        assertEquals(1, tableMetaModel.partitionColumns.size());
        assertEquals("bar", tableMetaModel.partitionColumns.get(0).odpsColumnName);
        assertEquals("string", tableMetaModel.partitionColumns.get(0).odpsType);
        assertEquals(0, tableMetaModel.partitions.size());
      }
    }
  }

  // TODO: test get config
}