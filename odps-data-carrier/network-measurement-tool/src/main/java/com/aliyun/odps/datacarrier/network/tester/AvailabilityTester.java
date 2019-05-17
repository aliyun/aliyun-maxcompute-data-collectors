package com.aliyun.odps.datacarrier.network.tester;

import com.aliyun.odps.datacarrier.network.Endpoint;
import com.aliyun.odps.datacarrier.network.summary.AvailabilitySummary;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AvailabilityTester {
  private static final Logger logger = LogManager.getLogger();

  public AvailabilitySummary test(Endpoint endpoint) {
    return isAvailable(endpoint);
  }

  public List<AvailabilitySummary> testAll(List<Endpoint> endpoints) {
    ArrayList<Callable<AvailabilitySummary>> callList = new ArrayList<>();
    for (Endpoint endpoint : endpoints) {
      Callable<AvailabilitySummary> call = () -> isAvailable(endpoint);
      callList.add(call);
    }

    ExecutorService pool = Executors.newFixedThreadPool(16);
    List<AvailabilitySummary> summaries = new ArrayList<>();
    try {
      List<Future<AvailabilitySummary>> futures = pool.invokeAll(callList);
      for (int i = 0; i < futures.size(); i++) {
        Future<AvailabilitySummary> future = futures.get(i);
        Endpoint endpoint = endpoints.get(i);
        AvailabilitySummary summary = future.get();
        summaries.add(summary);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }

    pool.shutdown();

    return summaries;
  }

  private AvailabilitySummary isAvailable(Endpoint endpoint) {
    AvailabilitySummary summary = new AvailabilitySummary(endpoint);

    // Equals 'curl http://odps.endpoint', timeout is 10 seconds
    HttpGet httpGet = new HttpGet(endpoint.getOdpsEndpoint());
    CloseableHttpClient client = getHttpClient(10 * 1000);

    int retry = 5;
    while (retry > 0) {
      try {
        StopWatch stopWatch = StopWatch.createStarted();
        CloseableHttpResponse response = client.execute(httpGet);
        stopWatch.stop();
        response.close();
        client.close();
        logger.debug("Connect to " + endpoint.getNetwork() + "-" + endpoint.getLocation() +
            " succeeded");
        summary.setAvailable(true);
        summary.setElapsedTime(stopWatch.getTime());
        break;
      } catch (Exception e) {
        retry -= 1;
      }
    }
    logger.debug("Connect to " + endpoint.getNetwork() + "-" + endpoint.getLocation() +
        " failed");
    return summary;
  }

  private CloseableHttpClient getHttpClient(int connectionTimeout) {
    RequestConfig.Builder configBuilder = RequestConfig.custom();
    configBuilder.setConnectTimeout(connectionTimeout);
    RequestConfig config = configBuilder.build();

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    clientBuilder.setDefaultRequestConfig(config);

    return clientBuilder.build();
  }
}
