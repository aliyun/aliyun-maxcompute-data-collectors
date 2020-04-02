package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

  private static final String ERROR_INDICATOR = "ERROR: ";
  private static final String WARNING_INDICATOR = "WARNING: ";

  /*
    Options
   */
  private static final String CONFIG_OPT = "config";
  private static final String START_OPT = "start";
  private static final String WAIT_OPT = "wait";
  private static final String WAIT_ALL_OPT = "wait_all";
  private static final String REMOVE_OPT = "remove";
  private static final String LIST_OPT = "list";
  private static final String HELP_OPT = "help";

  private static final String[] SUB_COMMANDS =
      new String[] {START_OPT, WAIT_OPT, WAIT_ALL_OPT, REMOVE_OPT, LIST_OPT};

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
    // TODO: prevent user from creating too many migration jobs
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

  @Override
  public void removeMigrationJob(String db, String tbl) {
    if (MmaMetaManagerFsImpl.getInstance().hasMigrationJob(db, tbl)) {
      MmaMetaManager.MigrationStatus status = MmaMetaManagerFsImpl.getInstance().getStatus(db, tbl);
      if (MmaMetaManager.MigrationStatus.PENDING.equals(status)) {
        String msg = String.format("Failed to remove migration job, database: %s, table: %s, "
                                   + "reason: status is RUNNING", db, tbl);
        LOG.error(msg);
        throw new IllegalArgumentException(ERROR_INDICATOR + msg);
      }
      MmaMetaManagerFsImpl.getInstance().removeMigrationJob(db, tbl);
    }

  }

  @Override
  public MmaMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl) {
    MmaMetaManager.MigrationStatus status =  MmaMetaManagerFsImpl.getInstance().getStatus(db, tbl);
    LOG.info("Get migration status, db: {}, tbl: {}, status: {}", db, tbl, status);

    return status;
  }

  @Override
  public MmaMetaManager.MigrationProgress getMigrationProgress(String db, String tbl) {
    MmaMetaManager.MigrationProgress progress =
        MmaMetaManagerFsImpl.getInstance().getProgress(db, tbl);
    LOG.info("Get migration progress, db: {}, tbl: {}, progress: {}",
             db, tbl, progress == null ? "N/A" : progress.toJson());

    return progress;
  }

  @Override
  public List<MmaConfig.TableMigrationConfig> listMigrationJobs(
      MmaMetaManager.MigrationStatus status) {

    List<MmaConfig.TableMigrationConfig> ret =
        MmaMetaManagerFsImpl.getInstance().listMigrationJobs(status);
    LOG.info("Get migration job list, status: {}, ret: {}",
             status,
             ret.stream()
                 .map(c -> c.getSourceDataBaseName() + "." + c.getSourceTableName())
                 .collect(Collectors.joining(", ")));
    return ret;
  }

  private static void logHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "mma-client";
    formatter.printHelp(cmdLineSyntax, options);
  }

  public static void main(String[] args) throws ParseException, IOException, MetaException {
    BasicConfigurator.configure();
    /*
      Required options
     */
    Option configOption = Option
        .builder(CONFIG_OPT)
        .longOpt(CONFIG_OPT)
        .argName(CONFIG_OPT)
        .hasArg()
        .desc("MMA client configuration, required")
        .build();
    /*
      Supported sub commands, mutually exclusive
     */
    Option startJobOption = Option
        .builder(START_OPT)
        .longOpt(START_OPT)
        .argName(START_OPT)
        .hasArg()
        .desc("Start a job with given config.json")
        .build();
    Option waitJobOption = Option
        .builder(WAIT_OPT)
        .longOpt(WAIT_OPT)
        .argName(WAIT_OPT)
        .hasArg()
        .desc("Wait until specified job completes")
        .build();
    Option waitAllOption = Option
        .builder(WAIT_ALL_OPT)
        .longOpt(WAIT_ALL_OPT)
        .hasArg(false)
        .desc("Wait until all job completes")
        .build();
    Option removeJobOption = Option
        .builder(REMOVE_OPT)
        .longOpt(REMOVE_OPT)
        .hasArg()
        .argName("<db>.<table>")
        .desc("Remove a migration job, its status should be succeeded or failed")
        .build();
    Option listJobsOption = Option
        .builder(LIST_OPT)
        .longOpt(LIST_OPT)
        .hasArg()
        .argName("PENDING | RUNNING | SUCCEEDED | FAILED")
        .desc("List migration jobs in given status")
        .build();

    /*
      Help
     */
    Option helpOption = Option
        .builder("h")
        .longOpt(HELP_OPT)
        .argName(HELP_OPT)
        .hasArg(false)
        .desc("Print usage")
        .build();

    Options options = new Options()
        .addOption(configOption)
        .addOption(startJobOption)
        .addOption(removeJobOption)
        .addOption(helpOption)
        .addOption(waitJobOption)
        .addOption(waitAllOption)
        .addOption(listJobsOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      logHelp(options);
      System.exit(0);
    }

    if (!cmd.hasOption(CONFIG_OPT)) {
      throw new IllegalArgumentException("Required argument 'config'");
    }

    Path mmaClientConfigFilePath = Paths.get(cmd.getOptionValue(CONFIG_OPT));
    MmaClientConfig mmaClientConfig = MmaClientConfig.fromFile(mmaClientConfigFilePath);
    if (!mmaClientConfig.validate()) {
      System.err.println("Invalid mma client config: " + mmaClientConfig.toJson());
      System.exit(1);
    }

    MmaClient client = new MmaClientFsImpl(mmaClientConfig);

    // Check if more than one sub command is given
    int numSubCommandSpecified = 0;
    for (String subCommand : SUB_COMMANDS) {
      if (cmd.hasOption(subCommand)) {
        numSubCommandSpecified += 1;
      }
    }
    if (numSubCommandSpecified > 1) {
      System.err.println("More than one option specified");
      System.exit(1);
    }

    if (cmd.hasOption(START_OPT)) {
      Path mmaMigrationConfigPath = Paths.get(cmd.getOptionValue(START_OPT));
      MmaMigrationConfig mmaMigrationConfig = MmaMigrationConfig.fromFile(mmaMigrationConfigPath);
      if (!mmaMigrationConfig.validate()) {
        System.err.println("Invalid mma migration config: " + mmaClientConfig.toJson());
        System.exit(1);
      }

      client.createMigrationJobs(mmaMigrationConfig);
      System.err.println("\nJob submitted");
    } else if (cmd.hasOption(WAIT_OPT)) {
      String jobName = cmd.getOptionValue(WAIT_OPT);
      int dotIdx = jobName.indexOf(".");
      String db = jobName.substring(0, dotIdx);
      String tbl = jobName.substring(dotIdx + 1);

      JobProgressReporter reporter = new JobProgressReporter();
      while (true) {
        MmaMetaManager.MigrationStatus status = client.getMigrationJobStatus(db, tbl);
        if (MmaMetaManager.MigrationStatus.FAILED.equals(status) ||
            MmaMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
          System.err.println("\nJob " + jobName + " " + status);
          break;
        } else {
          MmaMetaManager.MigrationProgress progress = client.getMigrationProgress(db, tbl);
          reporter.report(jobName, progress);
        }

        try {
          Thread.sleep(MMA_CLIENT_WAIT_INTERVAL);
        } catch (InterruptedException e) {
          System.err.println("Stop waiting, exit");
        }
      }
    } else if (cmd.hasOption(WAIT_ALL_OPT)) {
      JobProgressReporter reporter = new JobProgressReporter();
      while (true) {
        List<MmaConfig.TableMigrationConfig> runningMigrationJobs = client.listMigrationJobs(
            MmaMetaManager.MigrationStatus.PENDING);

        if (runningMigrationJobs.isEmpty()) {
          System.err.println("\nAll migration jobs succeeded");
          break;
        }

        Map<String, MmaMetaManager.MigrationProgress> tableToProgress = new HashMap<>();
        for (MmaConfig.TableMigrationConfig tableMigrationConfig : runningMigrationJobs) {
          String db = tableMigrationConfig.getSourceDataBaseName();
          String tbl = tableMigrationConfig.getSourceTableName();
          MmaMetaManager.MigrationProgress progress = client.getMigrationProgress(db, tbl);
          tableToProgress.put(db + "." + tbl, progress);
        }

        reporter.report(tableToProgress);
        try {
          Thread.sleep(MMA_CLIENT_WAIT_INTERVAL);
        } catch (InterruptedException e) {
          System.err.println("Stop waiting, exit");
        }
      }
    } else if (cmd.hasOption(REMOVE_OPT)) {
      String jobName = cmd.getOptionValue(REMOVE_OPT).trim();
      int dotIdx = jobName.indexOf(".");
      String db = jobName.substring(0, dotIdx);
      String tbl = jobName.substring(dotIdx + 1);

      client.removeMigrationJob(db, tbl);
    } else if (cmd.hasOption(LIST_OPT)) {
      String statusStr = cmd.getOptionValue(LIST_OPT).trim().toUpperCase();
      MmaMetaManager.MigrationStatus status = MmaMetaManager.MigrationStatus.valueOf(statusStr);

      List<MmaConfig.TableMigrationConfig> migrationJobs = client.listMigrationJobs(status);
      for (MmaConfig.TableMigrationConfig tableMigrationConfig : migrationJobs) {
        String sourceDb = tableMigrationConfig.getSourceDataBaseName();
        String sourceTbl = tableMigrationConfig.getSourceTableName();
        String destPjt = tableMigrationConfig.getDestProjectName();
        String destTbl = tableMigrationConfig.getDestTableName();
        System.err.println(String.format("%s.%s:%s.%s", sourceDb, sourceTbl, destPjt, destTbl));
      }
    } else {
      System.err.println("\nNo sub command specified, exiting");
    }

    System.exit(0);
  }
}
