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
import com.aliyun.odps.datacarrier.network.tester.Utils.ConcurrentProgressBar;
import com.aliyun.odps.datacarrier.network.tester.Utils.Downloader;
import com.aliyun.odps.datacarrier.network.tester.Utils.Uploader;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceTester {
  private static final Logger logger = LogManager.getLogger();

  /**
   * In total, upload/download 4 * 25 = 100 MB
   */
  private static final int TEST_STRING_SIZE = 4 * 1024 * 1024;
  private static final int NUM_RECORDS = 25;
  private static final String TEST_STRING = RandomStringUtils.randomAlphabetic(TEST_STRING_SIZE);
  private static final String TEST_TABLE = "ODPS_NETWORK_MEASUREMENT_TOOL_TEST_TBL";
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

  private void createTestTable() throws OdpsException {
    this.odps.tables().create(TEST_TABLE, SCHEMA, true);
  }

  private void deleteTestTable() throws OdpsException {
    this.odps.tables().delete(TEST_TABLE, true);
  }

  public PerformanceSummary test(Endpoint endpoint, int numThread) {
    this.odps.setEndpoint(endpoint.getOdpsEndpoint());
    if (endpoint.getTunnelEndpoint() != null) {
      this.tunnel.setEndpoint(endpoint.getTunnelEndpoint());
    }

    PerformanceSummary summary = new PerformanceSummary(endpoint);

    try {
      logger.info("Create table " + TEST_TABLE);
      createTestTable();
      if (numThread == 1) {
        summary = testSingleThread(endpoint);
      } else {
        summary = testMultiThread(endpoint, numThread);
      }
    } catch (Exception e) {
      logger.error("Error happened when upload/download: ");
      e.printStackTrace();
    } finally {
      try {
        logger.info("Delete table " + TEST_TABLE);
        deleteTestTable();
      } catch (OdpsException e) {
        // ignore
      }
    }

    return summary;
  }

  private PerformanceSummary testSingleThread(Endpoint endpoint) throws OdpsException, IOException {
    PerformanceSummary summary = new PerformanceSummary(endpoint);
    summary.setDataSizeMegaByte(getUploadDownloadMegaBytes(1));

    logger.info("Start testing upload performance");
    ConcurrentProgressBar uploadProgressBar = new ConcurrentProgressBar(NUM_RECORDS);
    try {
      long uploadElapsedTime = testSingleThreadUpload(uploadProgressBar);
      summary.setUploadElapsedTime(uploadElapsedTime);
    } finally {
      // Progress bar must close before print next log, or the output will be in a mess
      uploadProgressBar.close();
    }
    logger.info("Done testing upload performance");

    logger.info("Start testing download performance");
    ConcurrentProgressBar downloadProgressBar = new ConcurrentProgressBar(NUM_RECORDS);
    try {
      long downloadElapsedTime = testSingleThreadDownload(downloadProgressBar);
      summary.setDownloadElapsedTime(downloadElapsedTime);
    } finally {
      // Progress bar must close before print next log, or the output will be in a mess
      downloadProgressBar.close();
    }
    logger.info("Done testing download performance");

    return summary;
  }

  private PerformanceSummary testMultiThread(Endpoint endpoint, int numThread)
      throws InterruptedException, ExecutionException, TunnelException, IOException {
    PerformanceSummary summary = new PerformanceSummary(endpoint);
    summary.setDataSizeMegaByte(getUploadDownloadMegaBytes(numThread));

    logger.info("Start testing upload performance, number of thread: " + numThread);
    ConcurrentProgressBar uploadProgressBar =
        new ConcurrentProgressBar(NUM_RECORDS * numThread);
    try {
      long uploadElapsedTime = testMultiThreadUpload(numThread, uploadProgressBar);
      summary.setUploadElapsedTime(uploadElapsedTime);
    } finally {
      // Progress bar must close before print next log, or the output will be in a mess
      uploadProgressBar.close();
    }
    logger.info("Done testing upload performance");

    logger.info("Start testing download performance, number of thread: " + numThread);
    ConcurrentProgressBar downloadProgressBar =
        new ConcurrentProgressBar(NUM_RECORDS * numThread);
    try {
      long downloadElapsedTime = testMultiThreadDownload(numThread, downloadProgressBar);
      summary.setDownloadElapsedTime(downloadElapsedTime);
    } finally {
      // Progress bar must close before print next log, or the output will be in a mess
      downloadProgressBar.close();
    }
    logger.info("Done testing download performance");

    return summary;
  }

  public List<PerformanceSummary> testAll(List<Endpoint> endpoints, int numThread) {
    List<PerformanceSummary> summaries = new ArrayList<>();
    for (Endpoint endpoint : endpoints ) {
      summaries.add(test(endpoint, numThread));
    }

    return summaries;
  }

  private long testSingleThreadUpload(ConcurrentProgressBar progressBar)
      throws TunnelException, IOException {
    // Initialize tunnel record writer, disable compress option to get real performance and
    // set buffer size to 64 MB to boost performance
    UploadSession uploadSession = tunnel.createUploadSession(project, TEST_TABLE);
    RecordWriter writer = uploadSession.openBufferedWriter(false);
    ((TunnelBufferedWriter) writer).setBufferSize(16 * 1024 * 1024);

    // Same record will be reused to avoid overhead from CPU
    Record reusedRecord = uploadSession.newRecord();
    reusedRecord.set(0, TEST_STRING);

    // Upload 25 records, 100 MB in total
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
    return stopWatch.getTime();
  }

  private long testMultiThreadUpload(int numThread, ConcurrentProgressBar progressBar)
      throws InterruptedException, ExecutionException, TunnelException, IOException {
    List<Callable<Object>> callList = new ArrayList<>();
    UploadSession uploadSession = tunnel.createUploadSession(project, TEST_TABLE);
    for (int i = 0; i < numThread; i++) {
      RecordWriter writer = uploadSession.openBufferedWriter(false);
      ((TunnelBufferedWriter) writer).setBufferSize(16 * 1024 * 1024);

      // Same record will be reused to avoid overhead from CPU
      Record reusedRecord = uploadSession.newRecord();
      reusedRecord.set(0, TEST_STRING);

      Callable<Object> call = new Uploader(writer, reusedRecord, NUM_RECORDS, progressBar);
      callList.add(call);
    }

    ExecutorService pool = Executors.newFixedThreadPool(numThread);
    StopWatch stopWatch = StopWatch.createStarted();
    List<Future<Object>> futures = pool.invokeAll(callList);
    for (Future<Object> future : futures) {
      future.get();
    }
    uploadSession.commit();
    stopWatch.stop();
    pool.shutdown();

    return stopWatch.getTime();
  }

  private long testSingleThreadDownload(ConcurrentProgressBar progressBar)
      throws TunnelException, IOException {
    DownloadSession downloadSession = tunnel.createDownloadSession(project, TEST_TABLE);
    RecordReader reader = downloadSession.openRecordReader(0, NUM_RECORDS, false);

    Record reusedRecord = new ArrayRecord(SCHEMA);

    StopWatch stopWatch = StopWatch.createStarted();
    for (int i = 0; i < NUM_RECORDS; i++) {
      ((TunnelRecordReader) reader).read(reusedRecord);
      stopWatch.suspend();
      progressBar.step();
      stopWatch.resume();
    }
    reader.close();
    stopWatch.stop();
    return stopWatch.getTime();
  }

  private long testMultiThreadDownload(int numThread, ConcurrentProgressBar progressBar)
      throws TunnelException, IOException, ExecutionException, InterruptedException {
    List<Callable<Object>> callList = new ArrayList<>();
    DownloadSession downloadSession = tunnel.createDownloadSession(project, TEST_TABLE);

    for (int i = 0; i < numThread; i++) {
      RecordReader reader = downloadSession.openRecordReader(0, NUM_RECORDS, false);

      Callable<Object> call = new Downloader(reader, NUM_RECORDS, progressBar);
      callList.add(call);
    }

    ExecutorService pool = Executors.newFixedThreadPool(numThread);
    StopWatch stopWatch = StopWatch.createStarted();
    List<Future<Object>> futures = pool.invokeAll(callList);
    for (Future<Object> future : futures) {
      future.get();
    }
    stopWatch.stop();
    pool.shutdown();

    return stopWatch.getTime();
  }

  private static float getUploadDownloadMegaBytes(int numThread) {
    return numThread * NUM_RECORDS * ((float) TEST_STRING_SIZE) / 1024 / 1024;
  }
}
