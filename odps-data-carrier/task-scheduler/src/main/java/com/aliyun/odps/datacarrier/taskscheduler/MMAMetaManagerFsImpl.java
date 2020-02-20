package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.aliyun.odps.datacarrier.metacarrier.MetaSource;

/**
 * directory structure:
 * root
 * |____database name
 *      |____table name
 *           |____metadata_tbl (first line is status and second line is failed_times)
 *           |____config
 *           |____metadata_pt_[partition name] (first line is status and second line is failed_times)
 */
public class MMAMetaManagerFsImpl implements MMAMetaManager {

  private static final Logger LOG = LogManager.getLogger(MMAMetaManagerFsImpl.class);

  private static final String META_DIR_NAME = ".mma";
  private static final String TABLE_METADATA_FILE_PREFIX = "metadata_";
  private static final String PARTITION_METADATA_FILE_PREFIX = "metadata_pt_";

  private Path workspace;
  private MetaSource metaSource;

  // TODO: argument should includes a global mma config
  public MMAMetaManagerFsImpl(String parentDir, MetaSource metaSource) {
    if (parentDir == null) {
      parentDir = System.getProperty("user.dir");
    }

    workspace = Paths.get(parentDir, META_DIR_NAME).toAbsolutePath();
    this.metaSource = metaSource;
  }

  @Override
  public synchronized void initMigration(String db, String tbl, TableMigrationConfig config) {
    if (db == null || tbl == null || config == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'config' cannot be null");
    }

    List<List<String>> partitionValuesList;
    try {
      partitionValuesList = metaSource.listPartitions(db, tbl);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to get metadata, database: " +
                                         db + ", table: " + tbl);
    }

    initMigrationInternal(db, tbl, partitionValuesList, config);
  }

  @Override
  public synchronized void initMigration(String db,
                                         String tbl,
                                         List<List<String>> partitionValuesList,
                                         TableMigrationConfig config) {
    if (db == null || tbl == null || config == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'config' cannot be null");
    }

    initMigrationInternal(db, tbl, partitionValuesList, config);
  }

  private void initMigrationInternal(String db,
                                     String tbl,
                                     List<List<String>> partitionValuesList,
                                     TableMigrationConfig config) {
    Path migrationRootDir = Paths.get(workspace.toString(), db, tbl);
    Path metadataPath = getMetadataPath(db, tbl);
    Path configPath = getConfigPath(db, tbl);

    if (migrationRootDir.toFile().exists()) {
      LOG.info("Migration root dir exists, db: " + db +
               ", tbl: " + tbl + ", path" + migrationRootDir.toString());
      // If migration root dir exists and status is not running or pending, remove it and continue
      MigrationStatus status = getStatusInternal(metadataPath);
      if (status.equals(MigrationStatus.RUNNING) || status.equals(MigrationStatus.PENDING)) {
        throw new IllegalArgumentException("Failed to init migration, current status: "
                                           + status.toString());
      }
      // Remove the migration root dir and continue
      if (!DirUtils.removeDir(migrationRootDir)) {
        throw new IllegalStateException("Failed to init migration, remove "
                                        + migrationRootDir.toString() + " failed");
      }
    }

    try {
      String content = String.format("%s\n%d", MigrationStatus.PENDING.toString(), 0);
      DirUtils.writeFile(metadataPath, content);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to init migration, init "
                                      + metadataPath.toString() + " failed");
    }

    try {
      DirUtils.writeFile(configPath, config.toString());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to init migration, init "
                                      + metadataPath.toString() + " failed");
    }

    // Init partition metadata file
    if (partitionValuesList != null) {
      for (List<String> partitionValues : partitionValuesList) {
        Path partitionMetadataPath = getMetadataPath(db, tbl, partitionValues);
        try {
          String content = String.format("%s\n%d", MigrationStatus.PENDING.toString(), 0);
          DirUtils.writeFile(partitionMetadataPath, content);
        } catch (IOException e) {
          throw new IllegalStateException("Failed to init migration, init "
                                          + metadataPath.toString() + " failed");
        }
      }
    }
  }

  @Override
  public synchronized void updateStatus(String db, String tbl, MigrationStatus status) {
    if (db == null || tbl == null || status == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'status' cannot be null");
    }

    Path metadataPath = getMetadataPath(db, tbl);
    updateStatusInternal(metadataPath, status);
  }

  @Override
  public synchronized void updateStatus(String db, String tbl,
                                        List<List<String>> partitionValuesList,
                                        MigrationStatus status) {
    if (db == null || tbl == null || partitionValuesList == null || status == null) {
      throw new IllegalArgumentException(
          "'db' or 'tbl' or 'partitionValuesList' or 'status' cannot be null");
    }

    for (List<String> partitionValues : partitionValuesList) {
      Path partitionMetadataPath = getMetadataPath(db, tbl, partitionValues);
      updateStatusInternal(partitionMetadataPath, status);
    }
  }

  private void updateStatusInternal(Path metadataPath, MigrationStatus status) {
    if (!metadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to update status, file does not exist: "
                                      + metadataPath.toString());
    }

    int failedTimes;
    failedTimes = getFailedTimesInternal(metadataPath);

    try {
      DirUtils.writeFile(metadataPath,
                         String.format("%s\n%d", status.toString(), failedTimes));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to update status, write "
                                      + metadataPath.toString() + " failed");
    }
  }

  @Override
  public synchronized MigrationStatus getStatus(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    Path tableMetadataPath = getMetadataPath(db, tbl);
    return getStatusInternal(tableMetadataPath);
  }

  @Override
  public synchronized MigrationStatus getStatus(String db, String tbl, List<String> partitionValues) {
    if (db == null || tbl == null || partitionValues == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValues' cannot be null");
    }

    Path partitionMetadataPath = getMetadataPath(db, tbl, partitionValues);
    return getStatusInternal(partitionMetadataPath);
  }

  private MigrationStatus getStatusInternal(Path metadataPath) {
    if (!metadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to get status, file does not exist: "
                                      + metadataPath.toString());
    }

    String content;
    try {
      content = DirUtils.readFile(metadataPath);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read file: " + metadataPath.toString());
    }

    String[] lines = content.split("\n");
    if (lines.length != 2) {
      throw new IllegalArgumentException("Metadata corrupted, content: " + content);
    }

    return MigrationStatus.valueOf(lines[0].trim());
  }

  @Override
  public synchronized TableMigrationConfig getConfig(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    Path configPath = getConfigPath(db, tbl);
    return getConfigInternal(configPath);
  }

  public TableMigrationConfig getConfigInternal(Path configPath) {
    if (!configPath.toFile().exists()) {
      throw new IllegalStateException("Failed to get config, file does not exist: "
                                      + configPath.toString());
    }

    String content;
    try {
      content = DirUtils.readFile(configPath);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read file: " + configPath.toString());
    }

    return TableMigrationConfig.fromString(content);
  }

  @Override
  public synchronized int getFailedTimes(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    Path tableMetadataPath = getMetadataPath(db, tbl);
    return getFailedTimesInternal(tableMetadataPath);
  }

  @Override
  public synchronized int getFailedTimes(String db, String tbl, List<String> partitionValues) {
    if (db == null || tbl == null || partitionValues == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValues' cannot be null");
    }

    Path partitionMetadataPath = getMetadataPath(db, tbl, partitionValues);
    return getFailedTimesInternal(partitionMetadataPath);
  }

  public int getFailedTimesInternal(Path metadataPath) {
    if (!metadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to get failed times, file does not exist: "
                                      + metadataPath.toString());
    }

    String content;
    try {
      content = DirUtils.readFile(metadataPath);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read file: " + metadataPath.toString());
    }

    String[] lines = content.split("\n");
    if (lines.length != 2) {
      throw new IllegalArgumentException("Metadata corrupted, content: " + content);
    }

    return Integer.parseInt(lines[1].trim());
  }

  @Override
  public void increaseFailedTimes(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    Path metadataPath = getMetadataPath(db, tbl);
    increaseFailedTimesInternal(metadataPath);
  }

  @Override
  public void increaseFailedTimes(String db, String tbl, List<String> partitionValues) {
    if (db == null || tbl == null || partitionValues == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'partitionValues' cannot be null");
    }

    Path partitionMetadataPath = getMetadataPath(db, tbl, partitionValues);
    increaseFailedTimesInternal(partitionMetadataPath);
  }

  public void increaseFailedTimesInternal(Path metadataPath) {
    if (!metadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to get increase failed times, file does not exist: "
                                      + metadataPath.toString());
    }

    int failedTimes = getFailedTimesInternal(metadataPath);
    MigrationStatus status = getStatusInternal(metadataPath);
    try {
      DirUtils.writeFile(metadataPath, String.format("%s\n%d", status.toString(), failedTimes + 1));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to increase failed times, update " +
                                      metadataPath.toString() + " failed");
    }
  }

  @Override
  public synchronized List<MetaSource.TableMetaModel> getPendingTables() {
    List<MetaSource.TableMetaModel> ret = new LinkedList<>();

    for (String db : DirUtils.listDirs(Paths.get(workspace.toString()))) {
      for (String tbl : DirUtils.listDirs(Paths.get(workspace.toString(), db))) {
        Path metadataPath = getMetadataPath(db, tbl);
        MigrationStatus status = getStatusInternal(metadataPath);
        if (MigrationStatus.PENDING.equals(status)) {
          try {
            MetaSource.TableMetaModel tableMetaModel =
                metaSource.getTableMetaWithoutPartitionMeta(db, tbl);

            // For partition tables, add its pending partitions
            if (tableMetaModel.partitionColumns.size() > 0) {
              List<MetaSource.PartitionMetaModel> pendingPartitionMetaModels = new LinkedList<>();
              Path tableMetaDir = Paths.get(workspace.toString(), db, tbl);
              for (String pendingPartitionMetaFile :
                  DirUtils.listFiles(tableMetaDir, PARTITION_METADATA_FILE_PREFIX)) {
                Path partitionMetadataPath = Paths.get(workspace.toString(), db, tbl,
                                                       pendingPartitionMetaFile);
                if (MigrationStatus.PENDING.equals(getStatusInternal(partitionMetadataPath))) {
                  pendingPartitionMetaModels.add(metaSource.getPartitionMeta(
                      db, tbl, toPartitionValues(pendingPartitionMetaFile)));
                }
              }

              tableMetaModel.partitions = pendingPartitionMetaModels;
            }

            Path configPath = getConfigPath(db, tbl);
            TableMigrationConfig config = getConfigInternal(configPath);
            config.applyRules(tableMetaModel);
            ret.add(tableMetaModel);
          } catch (Exception e) {
            LOG.error("Get metadata form metasource failed, database: " + db + ", " +
                      "table: " + tbl + ", stacktrace: " + ExceptionUtils.getStackTrace(e));
          }
        }
      }
    }

    return ret;
  }

  @Override
  public MetaSource.TableMetaModel getNextPendingTable() {
    throw new UnsupportedOperationException();
  }

  /**
   * Utils
   */
  private Path getMetadataPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, TABLE_METADATA_FILE_PREFIX + tbl);
  }

  private Path getMetadataPath(String db, String tbl, List<String> partitionValues) {
    return Paths.get(workspace.toString(), db, tbl,
                     toPartitionMetadataFileName(partitionValues));
  }

  private Path getConfigPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, "config");
  }

  private String toPartitionMetadataFileName(List<String> partitionValues) {
    // TODO: better way to store partition values
    return PARTITION_METADATA_FILE_PREFIX + String.join("_%_", partitionValues);
  }

  private List<String> toPartitionValues(String partitionMetadataFileName) {
    String[] partitionValues = partitionMetadataFileName.substring(
        PARTITION_METADATA_FILE_PREFIX.length()).split("_%_");
    return Arrays.asList(partitionValues);
  }
}
