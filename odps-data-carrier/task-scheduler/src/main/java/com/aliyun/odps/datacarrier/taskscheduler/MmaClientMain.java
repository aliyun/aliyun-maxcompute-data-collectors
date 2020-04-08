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

    MmaClient client;
    try {
      client = new MmaClientDbImpl(mmaClientConfig);
    } catch (MmaException e) {
      System.err.println("Creating mma client failed: ");
      e.printStackTrace();
      return;
    }

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

      try {
        client.createMigrationJobs(mmaMigrationConfig);
      } catch (MmaException e) {
        System.err.println("Create migration job failed: ");
        e.printStackTrace();
        return;
      }
      System.err.println("\nJob submitted");
    } else if (cmd.hasOption(WAIT_OPT)) {
      String jobName = cmd.getOptionValue(WAIT_OPT);
      int dotIdx = jobName.indexOf(".");
      String db = jobName.substring(0, dotIdx);
      String tbl = jobName.substring(dotIdx + 1);

      JobProgressReporter reporter = new JobProgressReporter();
      while (true) {
        try {
          MmaMetaManager.MigrationStatus status = client.getMigrationJobStatus(db, tbl);
          if (MmaMetaManager.MigrationStatus.FAILED.equals(status) ||
              MmaMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
            System.err.println("\nJob " + jobName + " " + status);
            break;
          } else {
            MmaMetaManager.MigrationProgress progress = client.getMigrationProgress(db, tbl);
            reporter.report(jobName, progress);
          }
        } catch (MmaException e) {
          System.err.println("Get migration progress failed: ");
          e.printStackTrace();
          return;
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
        try {
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
        } catch (MmaException e) {
          System.err.println("Get migration progress failed: ");
          e.printStackTrace();
        }

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
      try {
        client.removeMigrationJob(db, tbl);
      } catch (MmaException e) {
        System.err.println("Remove migration job failed: ");
        e.printStackTrace();
      }
    } else if (cmd.hasOption(LIST_OPT)) {
      String statusStr = cmd.getOptionValue(LIST_OPT).trim().toUpperCase();
      MmaMetaManager.MigrationStatus status = MmaMetaManager.MigrationStatus.valueOf(statusStr);

      try {
        List<MmaConfig.TableMigrationConfig> migrationJobs = client.listMigrationJobs(status);

        for (MmaConfig.TableMigrationConfig tableMigrationConfig : migrationJobs) {
          String sourceDb = tableMigrationConfig.getSourceDataBaseName();
          String sourceTbl = tableMigrationConfig.getSourceTableName();
          String destPjt = tableMigrationConfig.getDestProjectName();
          String destTbl = tableMigrationConfig.getDestTableName();
          System.err.println(String.format("%s.%s:%s.%s", sourceDb, sourceTbl, destPjt, destTbl));
        }
      } catch (MmaException e) {
        System.err.println("List migration progress failed: ");
        e.printStackTrace();
      }
    } else {
      System.err.println("\nNo sub command specified, exiting");
    }

    System.exit(0);
  }
}
