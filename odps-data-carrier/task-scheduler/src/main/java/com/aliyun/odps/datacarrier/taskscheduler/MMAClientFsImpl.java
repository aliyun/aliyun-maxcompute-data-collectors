package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;

public class MMAClientFsImpl implements MMAClient {

  private MetaConfiguration configuration;

  public MMAClientFsImpl(MetaConfiguration configuration) throws MetaException, IOException {
    MetaConfiguration.HiveConfiguration hiveConfigurationConfig = configuration.getHiveConfiguration();
    MetaSource metaSource = new HiveMetaSource(hiveConfigurationConfig.getThriftAddr(),
                                               hiveConfigurationConfig.getKrbPrincipal(),
                                               hiveConfigurationConfig.getKeyTab(),
                                               hiveConfigurationConfig.getKrbSystemProperties());
    MMAMetaManagerFsImpl.init(null, metaSource);
    this.configuration = configuration;
  }

  @Override
  public void createMigrationJobs() {
    for (MetaConfiguration.TableGroup tableGroup : configuration.getTableGroups()) {
      for (MetaConfiguration.TableConfig tableConfig : tableGroup.getTableConfigs()) {
        MMAMetaManagerFsImpl.getInstance().addMigrationJob(tableConfig);
      }
    }
  }

  public static void main(String[] args) throws ParseException, IOException, MetaException {
    BasicConfigurator.configure();
    Option startJobOption = Option
        .builder("start")
        .longOpt("start")
        .argName("start")
        .hasArg()
        .desc("Start a job with given config.json")
        .build();

    Options options = new Options()
        .addOption(startJobOption);

    // TODO: support wait job and wait all

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("start")) {
      String configFilePath = cmd.getOptionValue("start");
      if (!configFilePath.startsWith("/")) {
        configFilePath = Paths.get(System.getProperty("user.dir"), configFilePath).toString();
      }
      File configFile = new File(configFilePath);
      MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
      MMAClient client = new MMAClientFsImpl(metaConfiguration);
      client.createMigrationJobs();
      System.err.println("Job submitted");
    }
  }
}
