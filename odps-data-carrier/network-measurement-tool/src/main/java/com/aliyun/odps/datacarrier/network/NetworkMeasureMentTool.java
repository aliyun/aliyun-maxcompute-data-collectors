package com.aliyun.odps.datacarrier.network;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.datacarrier.network.summary.AvailabilitySummary;
import com.aliyun.odps.datacarrier.network.summary.PerformanceSummary;
import com.aliyun.odps.datacarrier.network.tester.AvailabilityTester;
import com.aliyun.odps.datacarrier.network.tester.PerformanceTester;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkMeasureMentTool {

  private static void findAvailableEndpoint() {
    AvailabilityTester tester = new AvailabilityTester();
    List<Endpoint> endpoints = new ArrayList<>();

    endpoints.addAll(Endpoints.getVPCEndpoints());
    endpoints.addAll(Endpoints.getExternalEndpoints());
    endpoints.addAll(Endpoints.getClassicNetworkEndpoints());

    List<AvailabilitySummary> summaries = tester.testAll(endpoints);

    // Sort the list of summary in ascending order by the elapsed time
    summaries.sort((o1, o2) -> (int) (o1.getElapsedTime() - o2.getElapsedTime()));
    for (AvailabilitySummary summary : summaries) {
      if (summary.getAvailable()) {
        summary.print();
      }
    }
  }

  private static void testPerformance(Endpoint endpoint, String project,
      String accessId, String accessKey, int numThread) {
    PerformanceTester tester = new PerformanceTester(project, accessId, accessKey);
    PerformanceSummary summary = tester.test(endpoint, numThread);
    summary.print();
  }

  public static void main(String[] args) throws ParseException, OdpsException, IOException {
    Options options = buildOptions();

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help") || !cmd.hasOption("mode")) {
      HelpFormatter formatter = new HelpFormatter();
      String cmdLineSyntax = "network-measure-tool --mode FIND|TEST";
      formatter.printHelp(cmdLineSyntax, options);
      return;
    }

    String modeValue = cmd.getOptionValue("mode");
    if ("FIND".equalsIgnoreCase(modeValue)) {
      findAvailableEndpoint();
    } else if ("TEST".equalsIgnoreCase(modeValue)) {
      validateTestModeOptions(cmd);
      String projectValue = cmd.getOptionValue("project");
      String accessIdValue = cmd.getOptionValue("access-id");
      String accessKeyValue = cmd.getOptionValue("access-key");
      String endpointValue = cmd.getOptionValue("endpoint");
      String tunnelEndpointValue = cmd.getOptionValue("tunnel-endpoint");
      String numThreadValue = cmd.getOptionValue("num-thread");
      Endpoint endpointObj =
          new Endpoint(endpointValue, tunnelEndpointValue, null, null);

      int numThread;
      if (numThreadValue == null) {
        numThread = 1;
      } else {
        numThread = Integer.parseInt(numThreadValue);
      }
      if (numThread <= 0) {
        throw new IllegalArgumentException("Invalid number of thread: " + numThreadValue);
      }
      testPerformance(endpointObj, projectValue, accessIdValue, accessKeyValue, numThread);
    } else {
      throw new IllegalArgumentException("Invalid mode: " + modeValue);
    }
  }

  private static Options buildOptions() {
    Option mode =  Option
        .builder()
        .longOpt("mode")
        .argName("mode")
        .hasArg()
        .desc("FIND (find available endpoints) or TEST (test performance of a single endpoint)")
        .build();

    Option project = Option
        .builder()
        .longOpt("project")
        .argName("project")
        .hasArg()
        .desc("ODPS project name, required in TEST mode")
        .build();

    Option accessId = Option
        .builder("u")
        .longOpt("access-id")
        .argName("access-id")
        .hasArg()
        .desc("ODPS access id, required in TEST mode")
        .build();

    Option accessKey = Option
        .builder("p")
        .longOpt("access-key")
        .argName("access-key")
        .hasArg()
        .desc("ODPS access key, required in TEST mode")
        .build();

    Option endpoint = Option
        .builder()
        .longOpt("endpoint")
        .argName("endpoint")
        .hasArg()
        .desc("ODPS endpoint, required in TEST mode")
        .build();

    Option tunnelEndpoint = Option
        .builder()
        .longOpt("tunnel-endpoint")
        .argName("tunnel-endpoint")
        .hasArg()
        .desc("ODPS tunnel endpoint, optional")
        .build();

    Option numThread = Option
        .builder("t")
        .longOpt("num-thread")
        .argName("num-thread")
        .hasArg()
        .desc("Number of thread")
        .build();

    Option help = Option
        .builder("h")
        .longOpt("help")
        .argName("help")
        .desc("Print help information")
        .build();

    Options options = new Options();
    options.addOption(mode);
    options.addOption(project);
    options.addOption(accessId);
    options.addOption(accessKey);
    options.addOption(endpoint);
    options.addOption(tunnelEndpoint);
    options.addOption(help);
    options.addOption(numThread);

    return options;
  }

  private static void validateTestModeOptions(CommandLine cmd) {
    if (!cmd.hasOption("project")) {
      throw new IllegalArgumentException("Project is required in TEST mode");
    }

    if (!cmd.hasOption("access-id")) {
      throw new IllegalArgumentException("Access id is required in TEST mode");
    }

    if (!cmd.hasOption("access-key")) {
      throw new IllegalArgumentException("Access key is required in TEST mode");
    }

    if (!cmd.hasOption("endpoint")) {
      throw new IllegalArgumentException("Endpoint is required in TEST mode");
    }
  }
}
