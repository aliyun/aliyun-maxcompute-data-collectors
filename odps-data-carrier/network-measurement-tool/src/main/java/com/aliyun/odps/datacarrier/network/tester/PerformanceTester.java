package com.aliyun.odps.datacarrier.network.tester;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.datacarrier.network.Endpoint;
import com.aliyun.odps.datacarrier.network.summary.PerformanceSummary;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceTester {
  private static final Logger logger = LogManager.getLogger();
  private static final String TEST_TABLE = "ODPS_NETWORK_MEASUREMENT_TOOL_TEST_TBL";
  // Length of test string, 4 MB
  public static final int TEST_STRING_SIZE = 4 * 1024 * 1024;
  public static final int NUM_RECORDS = 250;
  private static final TableSchema SCHEMA = new TableSchema();
  static {
    SCHEMA.addColumn(new Column("COL1", OdpsType.STRING));
  }

  private Odps odps;
  private String project;
  private TableTunnel tunnel;

  public PerformanceTester(String project, String accessId, String accessKey) {
    AliyunAccount account = new AliyunAccount(accessId, accessKey);
    this.odps = new Odps(account);
    this.project = project;
    this.odps.setDefaultProject(project);
    this.tunnel = new TableTunnel(odps);
  }

  private void CreateTestTable() throws OdpsException {
    this.odps.tables().create(TEST_TABLE, SCHEMA, true);
  }

  private void DeleteTestTable() throws OdpsException {
    this.odps.tables().delete(TEST_TABLE, true);
  }

  public PerformanceSummary test(Endpoint endpoint) throws OdpsException {
    System.out.println(project);

    this.odps.setEndpoint(endpoint.getOdpsEndpoint());
    if (endpoint.getTunnelEndpoint() != null) {
      this.tunnel.setEndpoint(endpoint.getTunnelEndpoint());
    }

    PerformanceSummary summary = new PerformanceSummary(endpoint);

    logger.info("Create table " + TEST_TABLE);
    CreateTestTable();
    try {
      logger.info("Start testing upload performance");
      long uploadElapsedTime = testSingleThreadUpload();
      summary.setUploadElapsedTime(uploadElapsedTime);
      logger.info("Done testing upload performance");

      logger.info("Start testing download performance");
      long downloadElapsedTime = testSingleThreadDownload();
      summary.setDownloadElapsedTime(downloadElapsedTime);
      logger.info("Done testing download performance");
    } catch (Exception e) {
      logger.error("Error happened when upload/download: ");
      e.printStackTrace();
    }

    logger.info("Delete table " + TEST_TABLE);
    DeleteTestTable();

    return summary;
  }

  public List<PerformanceSummary> testAll(List<Endpoint> endpoints)
      throws OdpsException, IOException {
    List<PerformanceSummary> summaries = new ArrayList<>();
    for (Endpoint endpoint : endpoints ) {
      summaries.add(test(endpoint));
    }

    return summaries;
  }

  private long testSingleThreadUpload() throws TunnelException, IOException {
    // Initialize tunnel record writer, disable compress option to get real performance and
    // set buffer size to 64 MB to boost performance
    UploadSession uploadSession = tunnel.createUploadSession(project, TEST_TABLE);
    RecordWriter writer = uploadSession.openBufferedWriter(false);
    ((TunnelBufferedWriter) writer).setBufferSize(16 * 1024 * 1024);

    String testString = RandomStringUtils.randomAlphabetic(TEST_STRING_SIZE);

    // Same record will be reused to avoid overhead from CPU
    Record reusedRecord = uploadSession.newRecord();
    reusedRecord.set(0, testString);

    // Upload 250 records, 1000 MB in total
    ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
    progressBarBuilder.setInitialMax(NUM_RECORDS);
    progressBarBuilder.setStyle(ProgressBarStyle.ASCII);
    ProgressBar progressBar = progressBarBuilder.build();

    StopWatch stopWatch = StopWatch.createStarted();
    for (int i = 0; i < NUM_RECORDS; i++) {
      writer.write(reusedRecord);
      stopWatch.suspend();
      progressBar.step();
      stopWatch.resume();
    }
    ((TunnelBufferedWriter) writer).flush();
    writer.close();
    uploadSession.commit();
    stopWatch.stop();
    progressBar.close();
    return stopWatch.getTime();
  }

  private long testSingleThreadDownload() throws TunnelException, IOException {
    DownloadSession downloadSession = tunnel.createDownloadSession(project, TEST_TABLE);
    RecordReader reader = downloadSession.openRecordReader(0, NUM_RECORDS, false);

    Record reusedRecord = new ArrayRecord(SCHEMA);

    ProgressBarBuilder progressBarBuilder = new ProgressBarBuilder();
    progressBarBuilder.setInitialMax(NUM_RECORDS);
    progressBarBuilder.setStyle(ProgressBarStyle.ASCII);
    ProgressBar progressBar = progressBarBuilder.build();

    StopWatch stopWatch = StopWatch.createStarted();
    for (int i = 0; i < NUM_RECORDS; i++) {
      ((TunnelRecordReader) reader).read(reusedRecord);
      stopWatch.suspend();
      progressBar.step();
      stopWatch.resume();
    }
    reader.close();
    stopWatch.stop();
    progressBar.close();
    return stopWatch.getTime();
  }

  private float bytesToMegaBytes(int bytes) {
    return (float) bytes / 1024 / 1024;
  }
}
