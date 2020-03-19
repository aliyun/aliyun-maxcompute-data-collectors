package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaClientFsImpl implements MmaClient {

  private static final Logger LOG = LogManager.getLogger(MmaClient.class);

  private static final long MMA_CLIENT_WAIT_INTERVAL = 5000;
  private static final int MMA_CLIENT_PROGRESS_BAR_LENGTH = 20;
  private static final String[] PROGRESS_INDICATOR = new String[] {".  ", ".. ", "..."};

  private static final String ERROR_INDICATOR = "ERROR: ";
  private static final String WARNING_INDICATOR = "WARNING: ";

  private MetaSource metaSource;

  public MmaClientFsImpl(MmaClientConfig mmaClientConfig) throws MetaException, IOException {
    MmaConfig.HiveConfig hiveConfig = mmaClientConfig.getHiveConfig();
    metaSource = new HiveMetaSource(hiveConfig.getHmsThriftAddr(),
                                    hiveConfig.getKrbPrincipal(),
                                    hiveConfig.getKeyTab(),
                                    hiveConfig.getKrbSystemProperties());
    MmaMetaManagerFsImpl.init(null, metaSource);
  }

  @Override
  public void createMigrationJobs(MmaMigrationConfig mmaMigrationConfig) {
    MmaConfig.AdditionalTableConfig globalAdditionalTableConfig =
        mmaMigrationConfig.getGlobalAdditionalTableConfig();

    if (mmaMigrationConfig.getServiceMigrationConfig() != null) {
      MmaConfig.ServiceMigrationConfig serviceMigrationConfig =
          mmaMigrationConfig.getServiceMigrationConfig();

      List<String> databases;
      try {
        databases = metaSource.listDatabases();
      } catch (Exception e) {
        String msg = "Failed to create migration jobs";
        System.err.println(ERROR_INDICATOR + msg);
        LOG.error(msg, e);
        return;
      }

      for (String database : databases) {
        createDatabaseMigrationJob(database,
                                   serviceMigrationConfig.getDestProjectName(),
                                   globalAdditionalTableConfig);
      }
    } else if (mmaMigrationConfig.getDatabaseMigrationConfigs() != null) {
      for (MmaConfig.DatabaseMigrationConfig databaseMigrationConfig :
          mmaMigrationConfig.getDatabaseMigrationConfigs()) {
        String database = databaseMigrationConfig.getSourceDatabaseName();

        if (!databaseExists(database)) {
          continue;
        }

        // TODO: merge additional table config
        // Use global additional table config if database migration config doesn't contain one
        MmaConfig.AdditionalTableConfig databaseAdditionalTableConfig =
            databaseMigrationConfig.getAdditionalTableConfig();
        if (databaseAdditionalTableConfig == null) {
          databaseAdditionalTableConfig = globalAdditionalTableConfig;
        }

        createDatabaseMigrationJob(database,
                                   databaseMigrationConfig.getDestProjectName(),
                                   databaseAdditionalTableConfig);
      }
    } else {
      for (MmaConfig.TableMigrationConfig tableMigrationConfig :
          mmaMigrationConfig.getTableMigrationConfigs()) {
        // TODO: merge additional table config
        if (tableMigrationConfig.getAdditionalTableConfig() == null) {
          tableMigrationConfig.setAdditionalTableConfig(
              mmaMigrationConfig.getGlobalAdditionalTableConfig());
        }

        String database = tableMigrationConfig.getSourceDataBaseName();
        String table = tableMigrationConfig.getSourceTableName();

        if (databaseExists(database) && tableExists(database, table)) {
          MmaMetaManagerFsImpl.getInstance().addMigrationJob(tableMigrationConfig);
          LOG.info("Job submitted, database: {}, table: {}",
                   tableMigrationConfig.getSourceDataBaseName(),
                   tableMigrationConfig.getSourceTableName());
        }
      }
    }
  }

  private boolean databaseExists(String database) {
    try {
      if (!metaSource.hasDatabase(database)) {
        String msg = "Database " + database + " not found";
        System.err.println(WARNING_INDICATOR + msg);
        LOG.warn(msg);
        return false;
      }
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for database:" + database;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return false;
    }
    return true;
  }

  private boolean tableExists(String database, String table) {
    try {
      if (!metaSource.hasTable(database, table)) {
        String msg = "Table " + database + "." + table + " not found";
        System.err.println("WARNING: " + msg);
        LOG.warn(msg);
        return false;
      }
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for table: " + database + "." + table;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return false;
    }
    return true;
  }

  private void createDatabaseMigrationJob(String database,
                                          String project,
                                          MmaConfig.AdditionalTableConfig databaseAdditionalTableConfig) {
    List<String> tables;
    try {
      tables = metaSource.listTables(database);
    } catch (Exception e) {
      String msg = "Failed to create migration jobs for database:" + database;
      System.err.println(ERROR_INDICATOR + msg);
      LOG.error(msg, e);
      return;
    }

    for (String table : tables) {
      MmaConfig.TableMigrationConfig tableMigrationConfig =
          new MmaConfig.TableMigrationConfig(
              database,
              table,
              project,
              table,
              databaseAdditionalTableConfig);

      MmaMetaManagerFsImpl.getInstance().addMigrationJob(tableMigrationConfig);
      LOG.info("Job submitted, database: {}, table: {}", database, table);
    }
  }

  public MmaMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl) {
    MmaMetaManager.MigrationStatus status =  MmaMetaManagerFsImpl.getInstance().getStatus(db, tbl);
    LOG.info("Get migration status, db: {}, tbl: {}, status: {}", db, tbl, status);

    return status;
  }

  public MmaMetaManager.MigrationProgress getMigrationProgress(String db, String tbl) {
    MmaMetaManager.MigrationProgress progress =
        MmaMetaManagerFsImpl.getInstance().getProgress(db, tbl);
    LOG.info("Get migration progress, db: {}, tbl: {}, progress: {}", db, tbl, progress);

    return progress;
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "mma-client";
    formatter.printHelp(cmdLineSyntax, options);
  }

  public static void main(String[] args) throws ParseException, IOException, MetaException {
    BasicConfigurator.configure();
    Option configOption = Option
        .builder("config")
        .longOpt("config")
        .argName("config")
        .hasArg()
        .desc("MMA client configuration, required")
        .build();
    Option startJobOption = Option
        .builder("start")
        .longOpt("start")
        .argName("start")
        .hasArg()
        .desc("Start a job with given config.json")
        .build();
    Option waitJobOption = Option
        .builder("wait")
        .longOpt("wait")
        .argName("wait")
        .hasArg()
        .desc("Wait until specified job completes")
        .build();
    Option helpOption = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .hasArg(false)
        .desc("Print usage")
        .build();

    Options options = new Options()
        .addOption(configOption)
        .addOption(startJobOption)
        .addOption(helpOption)
        .addOption(waitJobOption);

    // TODO: support wait all

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      logHelp(options);
      System.exit(0);
    }

    if (!cmd.hasOption("config")) {
      throw new IllegalArgumentException("Required argument 'config'");
    }

    Path mmaClientConfigFilePath = Paths.get(cmd.getOptionValue("config"));
    MmaClientConfig mmaClientConfig = MmaClientConfig.fromFile(mmaClientConfigFilePath);
    if (!mmaClientConfig.validate()) {
      System.err.println("Invalid mma client config: " + mmaClientConfig.toJson());
      System.exit(1);
    }

    MmaClient client = new MmaClientFsImpl(mmaClientConfig);

    if (cmd.hasOption("start")) {
      Path mmaMigrationConfigPath = Paths.get(cmd.getOptionValue("start"));
      MmaMigrationConfig mmaMigrationConfig = MmaMigrationConfig.fromFile(mmaMigrationConfigPath);
      if (!mmaMigrationConfig.validate()) {
        System.err.println("Invalid mma migration config: " + mmaClientConfig.toJson());
        System.exit(1);
      }

      client.createMigrationJobs(mmaMigrationConfig);
      System.err.println("\nJob submitted");
    } else if (cmd.hasOption("wait")) {
      String jobName = cmd.getOptionValue("wait");
      int dotIdx = jobName.indexOf(".");
      String db = jobName.substring(0, dotIdx);
      String tbl = jobName.substring(dotIdx + 1);

      int progressIndicatorIdx = 0;
      while (true) {
        MmaMetaManager.MigrationStatus status = client.getMigrationJobStatus(db, tbl);
        if (MmaMetaManager.MigrationStatus.FAILED.equals(status) ||
            MmaMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
          System.err.println("\nJob " + jobName + " " + status);
          break;
        } else {
          MmaMetaManager.MigrationProgress progress = client.getMigrationProgress(db, tbl);

          System.err.print(
              "\rWaiting" + PROGRESS_INDICATOR[progressIndicatorIdx % PROGRESS_INDICATOR.length]);
          progressIndicatorIdx += 1;

          // Print progress for partition tables
          // Format: Waiting... | [*****               ] | 25.00%
          if (progress != null) {
            int all = progress.getNumPendingPartitions() + progress.getNumRunningPartitions() +
                      progress.getNumFailedPartitions() + progress.getNumSucceededPartitions();

            float succeededPercent;
            if (all == 0) {
              succeededPercent = 1;
            } else {
              succeededPercent = progress.getNumSucceededPartitions() / (float) all;
            }
            StringBuilder sb = new StringBuilder(" | [");
            for (int i = 0; i < MMA_CLIENT_PROGRESS_BAR_LENGTH; i++) {
              if (i > succeededPercent * MMA_CLIENT_PROGRESS_BAR_LENGTH) {
                sb.append(" ");
              } else {
                sb.append("*");
              }
            }
            sb.append("] | ").append(String.format("%.2f%%", succeededPercent * 100));
            System.err.print(sb.toString());
          }
        }

        try {
          Thread.sleep(MMA_CLIENT_WAIT_INTERVAL);
        } catch (InterruptedException e) {
          System.err.println("Stop waiting, exit");
        }
      }
    }
    System.exit(0);
  }
}
