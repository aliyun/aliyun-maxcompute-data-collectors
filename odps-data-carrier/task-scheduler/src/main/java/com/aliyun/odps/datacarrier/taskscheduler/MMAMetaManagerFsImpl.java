package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.commons.DirUtils;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

/**
 * directory structure:
 * root
 * |____database name
 *      |____table name
 *           |____metadata (first line is status and second line is failed_times)
 *           |____config
 *           |____partitions_all
 *           |____partitions_failed
 *           |____partitions_succeeded
 */
public class MMAMetaManagerFsImpl implements MMAMetaManager {

  private static final Logger LOG = LogManager.getLogger(MMAMetaManagerFsImpl.class);

  private static final String META_DIR_NAME = ".mma";
  private static final String PARTITION_LIST_ALL = "partitions_all";
  private static final String PARTITION_LIST_FAILED = "partitions_failed";
  private static final String PARTITION_LIST_SUCCEEDED = "partitions_succeeded";

  /**
   * Control process level access
   */
  private static class Lock {
    private RandomAccessFile ram;
    private FileLock lock;

    public Lock(RandomAccessFile ram, FileLock lock) {
      if (ram == null || lock == null) {
        throw new IllegalArgumentException("'fis' or 'lock' cannot be null");
      }

      this.ram = ram;

      this.lock = lock;
    }

    public void release() throws IOException {
      lock.release();
      ram.close();
    }
  }
  private static Lock lock;

  private Path workspace;
  private MetaSource metaSource;

  public MMAMetaManagerFsImpl(String parentDir, MetaSource metaSource) throws IOException {
    if (parentDir == null) {
      // TODO: use a fixed parent directory
      parentDir = System.getProperty("user.dir");
    }

    this.workspace = Paths.get(parentDir, META_DIR_NAME).toAbsolutePath();
    this.metaSource = metaSource;

    // Create lock file
    LOG.info("lock path: {}", getLockPath().toString());
    if (!getLockPath().toFile().exists()) {
      DirUtils.writeFile(getLockPath(), Long.toString(System.currentTimeMillis()));
    }
  }

  @Override
  public synchronized void initMigration(MetaConfiguration.TableConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("'config' cannot be null");
    }
    acquireLock();

    try {
      String db = config.sourceDataBase;
      String tbl = config.sourceTableName;

      Path tableMetaDir = Paths.get(workspace.toString(), db, tbl);
      Path metadataPath = getMetadataPath(db, tbl);
      Path configPath = getConfigPath(db, tbl);

      // If the dir already exists
      // TODO: maybe we should force overwrite existing dir
      if (tableMetaDir.toFile().exists()) {
        LOG.info("Migration root dir exists, db: " + db +
                 ", tbl: " + tbl + ", path" + tableMetaDir.toString());
        // If migration root dir exists and status is not running or pending, remove it and continue
        MigrationStatus status = getStatusInternal(db, tbl);
        if (status.equals(MigrationStatus.RUNNING) || status.equals(MigrationStatus.PENDING)) {
          throw new IllegalStateException("Failed to init migration, current status: "
                                          + status.toString());
        }
        // Remove the migration root dir and continue
        if (!DirUtils.removeDir(tableMetaDir)) {
          throw new IllegalStateException("Failed to init migration, remove "
                                          + tableMetaDir.toString() + " failed");
        }
      }

      // Init metadata file
      try {
        String content = String.format("%s\n%d", MigrationStatus.PENDING.toString(), 0);
        DirUtils.writeFile(metadataPath, content);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to init migration, write "
                                        + metadataPath.toString() + " failed");
      }

      // Save configuration
      try {
        DirUtils.writeFile(configPath, MetaConfiguration.TableConfig.toJson(config));
      } catch (IOException e) {
        throw new IllegalStateException("Failed to init config, write "
                                        + configPath.toString() + " failed");
      }

      // Create partitions_all, partitions_succeeded and partitions_failed for partitioned tables
      try {
        MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(db, tbl);

        if (tableMetaModel.partitionColumns.size() > 0) {
          Path allPartitionListPath = getAllPartitionListPath(db, tbl);
          Path succeededPartitionListPath = getSucceededPartitionListPath(db, tbl);
          Path failedPartitionListPath = getFailedPartitionListPath(db, tbl);

          // Create partitions_all, partitions_succeeded and partitions_failed for partitioned tables
          if (!allPartitionListPath.toFile().createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_ALL);
          }
          if (!succeededPartitionListPath.toFile().createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_SUCCEEDED);
          }
          if (!failedPartitionListPath.toFile().createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_FAILED);
          }

          // User doesn't specify any partition, get from HMS
          if (config.partitionValuesList == null || config.partitionValuesList.isEmpty()) {
            config.partitionValuesList = metaSource.listPartitions(db, tbl);
          }

          // Init PARTITION_LIST_ALL
          try {
            FileOutputStream fos = new FileOutputStream(allPartitionListPath.toFile());
            CsvWriter csvWriter = new CsvWriter(fos, ',', StandardCharsets.UTF_8);
            for (List<String> partitionValues : config.partitionValuesList) {
              csvWriter.writeRecord(partitionValues.toArray(new String[0]));
            }
            csvWriter.close();
          } catch (IOException e) {
            throw new IllegalStateException("Failed to init migration, init "
                                            + allPartitionListPath.toString() + " failed");
          }
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to get metadata, database: " + db + ", table: " +
                                        tbl + ", stack trace: " + ExceptionUtils.getStackTrace(e));
      }
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized void updateStatus(String db, String tbl, MigrationStatus status) {
    if (db == null || tbl == null || status == null) {
      throw new IllegalArgumentException("'db' or 'tbl' or 'status' cannot be null");
    }
    acquireLock();

    try {
      updateStatusInternal(db, tbl, status);
    } finally {
      releaseLock();
    }
  }

  private void updateStatusInternal(String db, String tbl, MigrationStatus status) {
    Path metadataPath = getMetadataPath(db, tbl);
    if (!metadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to update status, file does not exist: "
                                      + metadataPath.toString());
    }

    int failedTimes = getFailedTimesInternal(metadataPath);

    // If the status is FAILED, set the status to PENDING if retry is allowed
    if (MigrationStatus.FAILED.equals(status)) {
      failedTimes += 1;
      MetaConfiguration.TableConfig config = getConfigInternal(getConfigPath(db, tbl));
      if (failedTimes <= config.config.getRetryTimesLimit()) {
        status = MigrationStatus.PENDING;
      }
    }

    try {
      DirUtils.writeFile(metadataPath,
                         String.format("%s\n%d", status.toString(), failedTimes));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to update status, write "
                                      + metadataPath.toString() + " failed");
    }
  }

  @Override
  public synchronized void updateStatus(String db, String tbl,
                                        List<List<String>> partitionValuesList,
                                        MigrationStatus status) {
    if (db == null || tbl == null || partitionValuesList == null ||  status == null) {
      throw new IllegalArgumentException(
          "'db' or 'tbl' or 'partitionValuesList' or 'status' cannot be null");
    }
    acquireLock();

    try {
      Path partitionListPath;
      if (MigrationStatus.SUCCEEDED.equals(status)) {
        partitionListPath = getSucceededPartitionListPath(db, tbl);
      } else if (MigrationStatus.FAILED.equals(status)) {
        partitionListPath = getFailedPartitionListPath(db, tbl);
      } else {
        return;
      }

      try {
        // Use csv to read/write the partition list files, since the partition values may contain
        // special char
        FileOutputStream fos = new FileOutputStream(partitionListPath.toFile(), true);
        CsvWriter csvWriter = new CsvWriter(fos, ',', StandardCharsets.UTF_8);
        for (List<String> partitionValues : partitionValuesList) {
          csvWriter.writeRecord(partitionValues.toArray(new String[0]));
        }
        csvWriter.close();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to update status, write "
                                        + partitionListPath.toString() + " failed");
      }
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized MigrationStatus getStatus(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }
    acquireLock();

    try {
      return getStatusInternal(db, tbl);
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized MigrationStatus getStatus(String db,
                                                String tbl,
                                                List<String> partitionValues) {
    throw new UnsupportedOperationException();
  }

  private MigrationStatus getStatusInternal(String db, String tbl) {
    Path tableMetadataPath = getMetadataPath(db, tbl);
    if (!tableMetadataPath.toFile().exists()) {
      throw new IllegalStateException("Failed to get status, file does not exist: "
                                      + tableMetadataPath.toString());
    }

    String content;
    try {
      content = DirUtils.readFile(tableMetadataPath);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read file: " + tableMetadataPath.toString());
    }

    String[] lines = content.split("\n");
    if (lines.length != 2) {
      throw new IllegalArgumentException("Metadata corrupted, content: " + content);
    }

    return MigrationStatus.valueOf(lines[0].trim());
  }

  @Override
  public synchronized MetaConfiguration.TableConfig getConfig(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }
    acquireLock();

    try {
      Path configPath = getConfigPath(db, tbl);
      return getConfigInternal(configPath);
    } finally {
      releaseLock();
    }
  }

  private MetaConfiguration.TableConfig getConfigInternal(Path configPath) {
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

    return MetaConfiguration.TableConfig.fromJson(content);
  }

  private int getFailedTimesInternal(Path metadataPath) {
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
  public synchronized List<MetaSource.TableMetaModel> getPendingTables() {
    acquireLock();

    try {
      List<MetaSource.TableMetaModel> ret = new LinkedList<>();

      for (String db : DirUtils.listDirs(Paths.get(workspace.toString()))) {
        for (String tbl : DirUtils.listDirs(Paths.get(workspace.toString(), db))) {
          MigrationStatus status = getStatusInternal(db, tbl);
          if (!MigrationStatus.PENDING.equals(status)) {
            continue;
          }
          try {
            MetaSource.TableMetaModel tableMetaModel =
                metaSource.getTableMetaWithoutPartitionMeta(db, tbl);

            // For partition tables, add its pending partitions
            if (tableMetaModel.partitionColumns.size() > 0) {
              // Get partitions to migrate
              Set<String> allPartitionListRawLines =
                  new HashSet<>(getRawLinesFromAllPartitionList(db, tbl));
              Set<String> succeededPartitionListRawLines =
                  new HashSet<>(getRawLinesFromSucceededPartitionList(db, tbl));
              allPartitionListRawLines.removeAll(succeededPartitionListRawLines);

              // Convert to partition values
              StringReader reader = new StringReader(String.join("\n",
                                                                 allPartitionListRawLines));
              CsvReader csvReader = new CsvReader(reader);
              List<MetaSource.PartitionMetaModel> partitions = new LinkedList<>();
              while (csvReader.readRecord()) {
                MetaSource.PartitionMetaModel partitionMetaModel =
                    metaSource.getPartitionMeta(db, tbl, Arrays.asList(csvReader.getValues()));
                partitions.add(partitionMetaModel);
              }
              csvReader.close();
              reader.close();

              // Add to table meta model
              tableMetaModel.partitions = partitions;
            }

            Path configPath = getConfigPath(db, tbl);
            MetaConfiguration.TableConfig config = getConfigInternal(configPath);

            config.apply(tableMetaModel);

            ret.add(tableMetaModel);
          } catch (Exception e) {
            LOG.error("Get metadata form metasource failed, database: " + db + ", " +
                      "table: " + tbl + ", stacktrace: " + ExceptionUtils.getStackTrace(e));
          }
        }
      }

      for (MetaSource.TableMetaModel tableMetaModel : ret) {
        LOG.info("Pending table: {}", GsonUtils.getFullConfigGson().toJson(tableMetaModel));
      }

      return ret;
    } finally {
      releaseLock();
    }
  }

  @Override
  public MetaSource.TableMetaModel getNextPendingTable() {
    throw new UnsupportedOperationException();
  }

  /**
   * Utils
   */
  private Path getMetadataPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, "metadata");
  }

  private Path getConfigPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, "config");
  }

  private Path getAllPartitionListPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, PARTITION_LIST_ALL);
  }

  private Path getFailedPartitionListPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, PARTITION_LIST_FAILED);
  }

  private Path getSucceededPartitionListPath(String db, String tbl) {
    return Paths.get(workspace.toString(), db, tbl, PARTITION_LIST_SUCCEEDED);
  }

  private Path getLockPath() {
    return Paths.get(workspace.toString(), ".lock");
  }

  private List<String> getRawLinesFromAllPartitionList(String db, String tbl) {
    Path allPartitionListPath = getAllPartitionListPath(db, tbl);
    return getRawLinesFromPartitionList(allPartitionListPath);
  }

  private List<String> getRawLinesFromSucceededPartitionList(String db, String tbl) {
    Path succeededPartitionListPath = getSucceededPartitionListPath(db, tbl);
    if (succeededPartitionListPath.toFile().exists()) {
      return getRawLinesFromPartitionList(succeededPartitionListPath);
    }
    return new LinkedList<>();
  }

  private List<String> getRawLinesFromPartitionList(Path path) {
    try {
      FileReader fileReader = new FileReader(path.toFile());
      BufferedReader bufferedReader = new BufferedReader(fileReader);

      List<String> ret = new LinkedList<>();
      bufferedReader.lines().forEach(ret::add);
      bufferedReader.close();
      fileReader.close();
      return ret;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read " + path.toString());
    }
  }


  /**
   * Ensure the directory won't be corrupted multiple {@link MMAMetaManagerFsImpl} runs at the same
   * time
   * @throws IOException
   */
  private void acquireLock() {
    Path lockPath = getLockPath();
    if (!lockPath.toFile().exists()) {
      throw new IllegalStateException("lock file does not exist");
    }

    if (lock != null) {
      throw new IllegalStateException("lock is not null");
    }

    try {
      RandomAccessFile ram = new RandomAccessFile(lockPath.toFile(), "rw");
      FileLock fl = null;
      while (fl == null) {
        fl = ram.getChannel().tryLock();
        Thread.sleep(100);
      }
      lock = new Lock(ram, fl);
    } catch (IOException | InterruptedException e) {
      throw new IllegalStateException("Failed to acquire lock on file: " + lockPath.toString());
    }
  }

  private void releaseLock() {
    Path lockPath = getLockPath();
    if (!lockPath.toFile().exists()) {
      throw new IllegalStateException("lock file does not exist");
    }

    if (lock == null) {
      throw new IllegalStateException("lock is null");
    }

    try {
      lock.release();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to release lock on file: " + lockPath.toString() +
                                      ", stack: " + ExceptionUtils.getStackTrace(e));
    }
    lock = null;
  }
}
