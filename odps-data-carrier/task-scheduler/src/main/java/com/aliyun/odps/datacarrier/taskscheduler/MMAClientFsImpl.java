package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;

public class MMAClientFsImpl implements MMAClient {

  private static final long MMA_CLIENT_WAIT_INTERVAL = 5000;
  private static final String[] PROGRESS_INDICATOR = new String[] {".  ", ".. ", "..."};

  public MMAClientFsImpl(MetaConfiguration configuration) throws MetaException, IOException {
    MetaConfiguration.HiveConfiguration hiveConfigurationConfig = configuration.getHiveConfiguration();
    MetaSource metaSource = new HiveMetaSource(hiveConfigurationConfig.getHmsThriftAddr(),
                                               hiveConfigurationConfig.getKrbPrincipal(),
                                               hiveConfigurationConfig.getKeyTab(),
                                               hiveConfigurationConfig.getKrbSystemProperties());
    MMAMetaManagerFsImpl.init(null, metaSource);
  }

  @Override
  public void createMigrationJobs(MetaConfiguration configuration) {
    for (MetaConfiguration.TableGroup tableGroup : configuration.getTableGroups()) {
      for (MetaConfiguration.TableConfig tableConfig : tableGroup.getTableConfigs()) {
        MMAMetaManagerFsImpl.getInstance().addMigrationJob(tableConfig);
      }
    }
  }

  public MMAMetaManager.MigrationStatus getMigrationJobStatus(String db, String tbl) {
    return MMAMetaManagerFsImpl.getInstance().getStatus(db, tbl);
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

    File mmaClientConfigFile = new File(cmd.getOptionValue("config"));
    MetaConfiguration mmaClientConfig = MetaConfigurationUtils.readConfigFile(mmaClientConfigFile);
    MMAClient client = new MMAClientFsImpl(mmaClientConfig);

    if (cmd.hasOption("start")) {
      File configFile = new File(cmd.getOptionValue("start"));
      MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
      client.createMigrationJobs(metaConfiguration);
      System.err.println("Job submitted");
    } else if (cmd.hasOption("wait")) {
      String jobName = cmd.getOptionValue("wait");
      int dotIdx = jobName.indexOf(".");
      String db = jobName.substring(0, dotIdx);
      String tbl = jobName.substring(dotIdx + 1);

      int progressIndicatorIdx = 0;
      while (true) {
        MMAMetaManager.MigrationStatus status = client.getMigrationJobStatus(db, tbl);
        if (MMAMetaManager.MigrationStatus.FAILED.equals(status) ||
            MMAMetaManager.MigrationStatus.SUCCEEDED.equals(status)) {
          System.err.println("Job " + jobName + " " + status);
          break;
        } else {
          System.err.print("\rWaiting" + PROGRESS_INDICATOR[progressIndicatorIdx % PROGRESS_INDICATOR.length]);
          progressIndicatorIdx += 1;
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
