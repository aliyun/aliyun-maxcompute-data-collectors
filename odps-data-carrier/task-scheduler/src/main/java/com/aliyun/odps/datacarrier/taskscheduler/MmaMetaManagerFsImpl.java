package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.File;
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
public class MmaMetaManagerFsImpl implements MmaMetaManager {

  private static final Logger LOG = LogManager.getLogger(MmaMetaManagerFsImpl.class);

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

  private static MmaMetaManagerFsImpl instance;

  public static void init(String parentDir, MetaSource metaSource) throws IOException {
    if (instance != null) {
      throw new IllegalStateException("Cannot initialize twice");
    }

    instance = new MmaMetaManagerFsImpl(parentDir, metaSource);
  }

  public static MmaMetaManagerFsImpl getInstance() {
    if (instance == null) {
      throw new IllegalStateException("MMAMetaManager not initialized");
    }

    return instance;
  }

  private MmaMetaManagerFsImpl(String parentDir, MetaSource metaSource) throws IOException {
    if (parentDir == null) {
      parentDir = System.getenv("MMA_HOME");
      if (parentDir == null) {
        throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
      }
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
  public synchronized void addMigrationJob(MetaConfiguration.TableConfig config) {
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
      boolean metadataExists = tableMetaDir.toFile().exists();

      // If the dir already exists
      if (metadataExists) {
        MigrationStatus status = getStatusInternal(db, tbl);
        // If the migration job has ended, restart it. Otherwise, throw an exception
        if (!MigrationStatus.SUCCEEDED.equals(status) && !MigrationStatus.FAILED.equals(status)) {
          String errMsg = String.format(
              "Migration job for %s.%s already exists and is running, please wait", db, tbl);
          throw new IllegalArgumentException(errMsg);
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

      // Create partitions_all, partitions_succeeded and partitions_failed if not exists for
      // partitioned tables
      try {
        MetaSource.TableMetaModel tableMetaModel =
            metaSource.getTableMetaWithoutPartitionMeta(db, tbl);

        if (tableMetaModel.partitionColumns.size() > 0) {
          File allPartitionList = getAllPartitionListPath(db, tbl).toFile();
          File succeededPartitionList = getSucceededPartitionListPath(db, tbl).toFile();
          File failedPartitionList = getFailedPartitionListPath(db, tbl).toFile();

          if (!allPartitionList.exists() && !allPartitionList.createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_ALL);
          }
          if (!succeededPartitionList.exists() && !succeededPartitionList.createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_SUCCEEDED);
          }
          if (!failedPartitionList.exists() && !failedPartitionList.createNewFile()) {
            throw new IllegalStateException("Failed to create file " + PARTITION_LIST_FAILED);
          }

          // User doesn't specify any partition, get from HMS
          List<List<String>> partitionValuesList = new LinkedList<>();
          if (config.partitionValuesList == null || config.partitionValuesList.isEmpty()) {
            partitionValuesList.addAll(metaSource.listPartitions(db, tbl));
          } else {
            if (metadataExists) {
              partitionValuesList.addAll(
                  getMergedAllPartitionValuesList(db, tbl, config.partitionValuesList));
            } else {
              // TODO: should check if config.partitionValuesList has duplications
              partitionValuesList.addAll(config.partitionValuesList);
            }
          }

          // Init PARTITION_LIST_ALL
          try {
            DirUtils.writeCsvFile(allPartitionList.toPath(), partitionValuesList);
          } catch (IOException e) {
            throw new IllegalStateException("Failed to init migration, init "
                                            + allPartitionList.toPath().toString() + " failed");
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
  public synchronized void removeMigrationJob(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }
    acquireLock();

    try {
      Path tableMetaDir = Paths.get(workspace.toString(), db, tbl);
      if (!tableMetaDir.toFile().exists()) {
        String errMsg = String.format("Migration job for %s.%s does not exist", db, tbl);
        throw new IllegalArgumentException(errMsg);
      }

      if (!DirUtils.removeDir(tableMetaDir)) {
        throw new IllegalStateException("Failed to remove dir: " + tableMetaDir.toString());
      }
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized boolean hasMigrationJob(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }
    acquireLock();

    try {
      Path tableMetaDir = Paths.get(workspace.toString(), db, tbl);
      return tableMetaDir.toFile().exists();
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
        DirUtils.appendToCsvFile(partitionListPath, partitionValuesList);
      } catch (IOException e) {
        throw new IllegalStateException("Failed to update status, write "
                                        + partitionListPath.toString() + " failed");
      }
    } finally {
      releaseLock();
    }

//    // Update the table migration status to SUCCEEDED if all partitions succeeds
//    if (MigrationStatus.SUCCEEDED.equals(status) &&
//        getUnSucceededPartitionValuesList(db, tbl).isEmpty()) {
//      updateStatusInternal(db, tbl, MigrationStatus.SUCCEEDED);
//    }
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

  @Override
  public MigrationProgress getProgress(String db, String tbl) {
    if (db == null || tbl == null) {
      throw new IllegalArgumentException("'db' or 'tbl' cannot be null");
    }

    try {
      MetaSource.TableMetaModel tableMetaModel =
          metaSource.getTableMetaWithoutPartitionMeta(db, tbl);
      if (tableMetaModel.partitionColumns.isEmpty()) {
        return null;
      }
    } catch (Exception e) {
      LOG.error("Get metadata form metasource failed, database: " + db + ", " +
                "table: " + tbl + ", stacktrace: " + ExceptionUtils.getStackTrace(e));
      return null;
    }

    acquireLock();
    try {
      int numAllPartitions = DirUtils.readLines(getAllPartitionListPath(db, tbl)).size();
      int numSucceedPartitions = DirUtils.readLines(getSucceededPartitionListPath(db, tbl)).size();
      // TODO: returned progress is simplified due to current impl
      return new MigrationProgress(0,
                                   numAllPartitions - numSucceedPartitions,
                                   numSucceedPartitions,
                                   0);
    } catch (IOException e) {
      LOG.error("Failed to get progress, database: " + db + ", " + "table: " + tbl);
      return null;
    } finally {
      releaseLock();
    }
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
              List<List<String>> partitionValuesList = getUnSucceededPartitionValuesList(db, tbl);
              List<MetaSource.PartitionMetaModel> partitions = new LinkedList<>();
              for (List<String> partitionValues : partitionValuesList) {
                MetaSource.PartitionMetaModel partitionMetaModel =
                    metaSource.getPartitionMeta(db, tbl, partitionValues);
                partitions.add(partitionMetaModel);
              }

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
        LOG.debug("Pending table: {}", GsonUtils.getFullConfigGson().toJson(tableMetaModel));
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

  private List<List<String>> getMergedAllPartitionValuesList(
      String db,
      String tbl,
      List<List<String>> newPartitionValuesList) {
    try {
      List<List<String>> allPartitionValuesList =
          DirUtils.readCsvFile(getAllPartitionListPath(db, tbl));
      for (List<String> partitionValues : newPartitionValuesList) {
        if (!allPartitionValuesList.contains(partitionValues)) {
          allPartitionValuesList.add(partitionValues);
        }
      }

      return allPartitionValuesList;
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to get merged all partition list, db:"
                                         + db + ", tbl: " + tbl);
    }
  }

  private List<List<String>> getUnSucceededPartitionValuesList(String db, String tbl) {
    try {
      Set<String> all = new HashSet<>(DirUtils.readLines(getAllPartitionListPath(db, tbl)));
      Set<String> succeeded =
          new HashSet<>(DirUtils.readLines(getSucceededPartitionListPath(db, tbl)));
      all.removeAll(succeeded);

      // Convert to partition values
      StringReader reader = new StringReader(String.join("\n", all));
      CsvReader csvReader = new CsvReader(reader);
      List<List<String>> ret = new LinkedList<>();
      while (csvReader.readRecord()) {
        ret.add(Arrays.asList(csvReader.getValues()));
      }
      csvReader.close();
      reader.close();
      return ret;
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to get difference of all partition list and "
                                         + "succeeded partition list, db:" + db + ", tbl: " + tbl);
    }
  }


  /**
   * Ensure the directory won't be corrupted multiple {@link MmaMetaManagerFsImpl} runs at the same
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
