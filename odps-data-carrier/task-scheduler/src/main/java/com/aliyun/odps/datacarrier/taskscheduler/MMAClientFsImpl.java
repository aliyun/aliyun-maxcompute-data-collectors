package com.aliyun.odps.datacarrier.taskscheduler;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.META_CONFIG_FILE;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;

public class MMAClientFsImpl implements MMAClient {

  private MMAMetaManager mmaMetaManager;
  private MetaConfiguration configuration;

  public MMAClientFsImpl(MetaConfiguration configuration) throws MetaException, IOException {
    MetaConfiguration.HiveConfiguration hiveConfigurationConfig = configuration.getHiveConfiguration();
    MetaSource metaSource = new HiveMetaSource(hiveConfigurationConfig.getThriftAddr(),
                                               hiveConfigurationConfig.getKrbPrincipal(),
                                               hiveConfigurationConfig.getKeyTab(),
                                               hiveConfigurationConfig.getKrbSystemProperties());
    this.mmaMetaManager = new MMAMetaManagerFsImpl(null, metaSource);
    this.configuration = configuration;
  }

  @Override
  public void createMigrationJobs() {
    for (MetaConfiguration.TableGroup tableGroup : configuration.getTableGroups()) {
      for (MetaConfiguration.TableConfig tableConfig : tableGroup.getTableConfigs()) {
        this.mmaMetaManager.initMigration(tableConfig);
      }
    }
  }

  public static void main(String[] args) throws ParseException, IOException, MetaException {
    BasicConfigurator.configure();
    Option config = Option
        .builder("config")
        .longOpt(META_CONFIG_FILE)
        .argName(META_CONFIG_FILE)
        .hasArg()
        .desc("Specify config.json, default: ./config.json")
        .build();

    Options options = new Options()
        .addOption(config);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);


    if (cmd.hasOption(META_CONFIG_FILE)) {
      File configFile = new File(System.getProperty("user.dir"), META_CONFIG_FILE);
      configFile = new File(cmd.getOptionValue(META_CONFIG_FILE));
      MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
      MMAClient client = new MMAClientFsImpl(metaConfiguration);
      client.createMigrationJobs();
      System.err.println("Job submitted");
    }
  }
}
