package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;



public class MmaServerMain {
  /*
    Options
   */
  private static final String CONFIG_OPT = "config";
  private static final String HELP_OPT = "help";

  /**
   * Print help info.
   *
   * @return Always return 0.
   */
  private static int help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "mma-server";
    formatter.printHelp(cmdLineSyntax, options);
    return 0;
  }

  public static void main(String[] args)
      throws ParseException, IOException, MetaException, MmaException {
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
        .addOption(helpOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      System.exit(help(options));
    }

    Path mmaServerConfigPath = Paths.get(cmd.getOptionValue("config"));
    MmaServerConfig.init(mmaServerConfigPath);

    MmaServer mmaServer = new MmaServer();
    try {
      mmaServer.run();
    } finally {
      mmaServer.shutdown();
    }
  }
}
