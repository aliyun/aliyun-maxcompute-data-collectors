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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;

public class MmaClientMain {
  private static final long MMA_CLIENT_WAIT_INTERVAL = 5000;

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


  /**
   * Print help info.
   *
   * @return Always return 0.
   */
  private static int help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "mma-client";
    formatter.printHelp(cmdLineSyntax, options);
    return 0;
  }

  /**
   * Start a migration job.
   *
   * @return If the job submitted successfully, returns 0, otherwise, returns 1.
   */
  private static int start(MmaClient client, String mmaMigrationConfigPathStr) {
    Path mmaMigrationConfigPath = Paths.get(mmaMigrationConfigPathStr);

    MmaMigrationConfig mmaMigrationConfig;
    try {
      mmaMigrationConfig = MmaMigrationConfig.fromFile(mmaMigrationConfigPath);
    } catch (IOException e) {
      e.printStackTrace();
      return 1;
    }

    if (!mmaMigrationConfig.validate()) {
      throw new IllegalArgumentException(
          "Invalid mma migration config: " + mmaMigrationConfig.toJson());
    }

    try {
      client.createMigrationJobs(mmaMigrationConfig);
    } catch (MmaException e) {
      e.printStackTrace();
      return 1;
    }

    System.err.println("\nJob submitted");
    return 0;
  }


  /**
   * Wait until a migration job ends.
   *
   * @return If the job succeeded, returns 0. If the job failed, returns -1. If any exception
   * happens during waiting returns 1.
   */
  private static int wait(MmaClient client, String jobName) {
    int dotIdx = jobName.indexOf(".");
    String db = jobName.substring(0, dotIdx);
    String tbl = jobName.substring(dotIdx + 1);

    JobProgressReporter reporter = new JobProgressReporter();
    while (true) {
      MmaMetaManager.MigrationStatus status;
      try {
        status = client.getMigrationJobStatus(db, tbl);
      } catch (MmaException e) {
        e.printStackTrace();
        return 1;
      }

      if (MmaMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
        System.err.println("Job " + jobName + " succeeded");
        return 0;
      } else if (MmaMetaManager.MigrationStatus.FAILED.equals(status))
      {
        System.err.println("Job " + jobName + " failed");
        return -1;
      } else {
        MmaMetaManager.MigrationProgress progress;
        try {
          progress = client.getMigrationProgress(db, tbl);
        } catch (MmaException e) {
          e.printStackTrace();
          return 1;
        }
        reporter.report(jobName, progress);
      }

      try {
        Thread.sleep(MMA_CLIENT_WAIT_INTERVAL);
      } catch (InterruptedException e) {
        return 1;
      }
    }
  }

  /**
   * Wait until all migration jobs end.
   *
   * @return If all jobs ended, returns 0. If any exception happens during waiting, returns 1.
   */
  private static int waitAll(MmaClient client) {
    JobProgressReporter reporter = new JobProgressReporter();
    while (true) {
      List<MmaConfig.JobConfig> runningJobs;
      try {
        runningJobs = client.listJobs(MmaMetaManager.MigrationStatus.RUNNING);
        runningJobs.addAll(client.listJobs(MmaMetaManager.MigrationStatus.PENDING));

        if (runningJobs.isEmpty()) {
          System.err.println("\nAll migration jobs terminated");
          return 0;
        }

        Map<String, MmaMetaManager.MigrationProgress> tableToProgress = new HashMap<>();
        for (MmaConfig.JobConfig config : runningJobs) {
          if (MmaConfig.JobType.MIGRATION.equals(config.getJobType())) {
            MmaConfig.TableMigrationConfig tableMigrationConfig =
                MmaConfig.TableMigrationConfig.fromJson(config.getDescription());
            String db = tableMigrationConfig.getSourceDataBaseName();
            String tbl = tableMigrationConfig.getSourceTableName();
            MmaMetaManager.MigrationProgress progress = client.getMigrationProgress(db, tbl);
            tableToProgress.put(db + "." + tbl, progress);
          } else if (MmaConfig.JobType.BACKUP.equals(config.getJobType())) {
            MmaConfig.ObjectExportConfig objectExportConfig =
                MmaConfig.ObjectExportConfig.fromJson(config.getDescription());
            String db = objectExportConfig.getDatabaseName();
            String tbl = objectExportConfig.getObjectName();
            tableToProgress.put(db + "." + tbl, null);
          } else if (MmaConfig.JobType.RESTORE.equals(config.getJobType())) {
             MmaConfig.ObjectRestoreConfig objectRestoreConfig =
                MmaConfig.ObjectRestoreConfig.fromJson(config.getDescription());
            String db = objectRestoreConfig.getDestinationDatabaseName();
            String tbl = objectRestoreConfig.getObjectName();
            tableToProgress.put(db + "." + tbl, null);
          }
        }

        reporter.report(tableToProgress);
      } catch (MmaException e) {
        e.printStackTrace();
        return 1;
      }

      try {
        Thread.sleep(MMA_CLIENT_WAIT_INTERVAL);
      } catch (InterruptedException e) {
        System.err.println("Stop waiting, exit");
      }
    }
  }

  /**
   * Remove a migration job.
   *
   * @return If the migration job is removed successfully, returns 0, otherwise, returns 1.
   */
  private static int remove(MmaClient client, String jobName) {
    int dotIdx = jobName.indexOf(".");
    String db = jobName.substring(0, dotIdx);
    String tbl = jobName.substring(dotIdx + 1);

    try {
      client.removeMigrationJob(db, tbl);
      return 0;
    } catch (MmaException e) {
      e.printStackTrace();
      return 1;
    }
  }

  /**
   * List migrations jobs in given status.
   *
   * @return If list migration jobs successfully, returns 0, otherwise, returns 1.
   */
  private static int list(MmaClient client, String statusStr) {
    MmaMetaManager.MigrationStatus status;
    if ("ALL".equalsIgnoreCase(statusStr)) {
      status = null;
    } else {
      status = MmaMetaManager.MigrationStatus.valueOf(statusStr);
    }

    List<MmaConfig.JobConfig> allJobs;
    try {
      allJobs = client.listJobs(status);
    } catch (MmaException e) {
      e.printStackTrace();
      return 1;
    }

    for (MmaConfig.JobConfig config : allJobs) {
      if (MmaConfig.JobType.MIGRATION.equals(config.getJobType())) {
        MmaConfig.TableMigrationConfig tableMigrationConfig =
            MmaConfig.TableMigrationConfig.fromJson(config.getDescription());
        System.err.println(String.format("[TableMigration] %s.%s:%s.%s",
            tableMigrationConfig.getSourceDataBaseName(),
            tableMigrationConfig.getSourceTableName(),
            tableMigrationConfig.getDestProjectName(),
            tableMigrationConfig.getDestTableName()));
      } else if (MmaConfig.JobType.BACKUP.equals(config.getJobType())) {
        MmaConfig.ObjectExportConfig objectExportConfig =
            MmaConfig.ObjectExportConfig.fromJson(config.getDescription());
        System.err.println(String.format("[MetaBackup] %s: %s.%s",
            objectExportConfig.getObjectType(),
            objectExportConfig.getDatabaseName(),
            objectExportConfig.getObjectName()));
      } else if (MmaConfig.JobType.RESTORE.equals(config.getJobType())) {
        MmaConfig.ObjectRestoreConfig objectRestoreConfig =
            MmaConfig.ObjectRestoreConfig.fromJson(config.getDescription());
        System.err.println(String.format("[Restore] %s: %s.%s",
            objectRestoreConfig.getObjectType(),
            objectRestoreConfig.getDestinationDatabaseName(),
            objectRestoreConfig.getObjectName()));
      }
    }

    return 0;
  }

  public static void main(String[] args) throws ParseException, IOException, MetaException {
    BasicConfigurator.configure();

    String mmaHome = System.getenv("MMA_HOME");
    if (mmaHome == null) {
      throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
    }

    Option configOption = Option
        .builder(CONFIG_OPT)
        .longOpt(CONFIG_OPT)
        .argName(CONFIG_OPT)
        .hasArg()
        .desc("MMA client configuration")
        .build();
    /*
      Sub commands, mutually exclusive
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
        .argName("ALL | PENDING | RUNNING | SUCCEEDED | FAILED")
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
      System.exit(help(options));
    }

    Path mmaClientConfigFilePath;
    if (!cmd.hasOption(CONFIG_OPT)) {
      mmaClientConfigFilePath = Paths.get(mmaHome, "conf", "mma_client_config.json");
    } else {
      mmaClientConfigFilePath = Paths.get(cmd.getOptionValue(CONFIG_OPT));
    }

    MmaServerConfig.init(mmaClientConfigFilePath);

    // Check if more than one sub command is given
    int numSubCommands = 0;
    for (String subCommand : SUB_COMMANDS) {
      if (cmd.hasOption(subCommand)) {
        numSubCommands += 1;
      }
    }
    if (numSubCommands > 1) {
      System.err.println("Found more than one option");
      System.exit(1);
    }

    MmaClient client;
    try {
      client = new MmaClientDbImpl();

      if (cmd.hasOption(START_OPT)) {
        System.exit(start(client, cmd.getOptionValue(START_OPT).trim()));
      } else if (cmd.hasOption(WAIT_OPT)) {
        System.exit(wait(client, cmd.getOptionValue(WAIT_OPT).trim()));
      } else if (cmd.hasOption(WAIT_ALL_OPT)) {
        System.exit(waitAll(client));
      } else if (cmd.hasOption(REMOVE_OPT)) {
        System.exit(remove(client, cmd.getOptionValue(REMOVE_OPT).trim()));
      } else if (cmd.hasOption(LIST_OPT)) {
        System.exit(list(client, cmd.getOptionValue(LIST_OPT).trim().toUpperCase()));
      } else {
        System.err.println("\nNo sub command specified, exiting");
        System.exit(1);
      }
    } catch (MmaException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }
}
