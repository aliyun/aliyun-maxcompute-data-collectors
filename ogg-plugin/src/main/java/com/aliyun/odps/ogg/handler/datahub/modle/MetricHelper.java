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

package com.aliyun.odps.ogg.handler.datahub.modle;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetricHelper {
    private final static Logger logger = LoggerFactory.getLogger(MetricHelper.class);

    private Configure configure;
    private ScheduledExecutorService scheduledExecutorService;

    private long start;
    private AtomicLong buildTime;
    private AtomicLong sendTime;
    private AtomicLong sendCount;
    private AtomicLong handleTime;
    private AtomicLong commitTime;
    private AtomicLong recordNum;
    private AtomicLong commitNum;


    private static MetricHelper metricHelper;

    public static MetricHelper instance() {
        return metricHelper;
    }

    public static void init(Configure configure) {
        if (metricHelper == null) {
            metricHelper = new MetricHelper(configure);
        }
    }

    public static void destroy() {
        if (metricHelper != null) {
            metricHelper.stop();
        }
        metricHelper = null;
    }

    private void stop() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    private MetricHelper(Configure configure) {
        this.configure = configure;

        buildTime = new AtomicLong();
        sendTime = new AtomicLong();
        sendCount = new AtomicLong();
        handleTime = new AtomicLong();
        commitTime = new AtomicLong();
        recordNum = new AtomicLong();
        commitNum = new AtomicLong();
        reset();

        scheduledExecutorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("DataHub.Metric-%d").daemon(true).build());

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            report();
            reset();
        }, configure.getReportMetricIntervalMs(), configure.getReportMetricIntervalMs(), TimeUnit.MILLISECONDS);
    }

    public void addBuildTime(long time) {
        buildTime.getAndAdd(time);
    }

    public void addSend(long time) {
        sendTime.getAndAdd(time);
        sendCount.getAndIncrement();
    }

    public void addHandle(long time) {
        handleTime.getAndAdd(time);
        recordNum.getAndAdd(time);
    }

    public void addCommit(long time) {
        commitTime.getAndAdd(time);
        commitNum.getAndIncrement();
    }

    private void report() {
        StringBuilder builder = new StringBuilder(256);
        builder.append("\n\t      ************* metric report *************\n");
        builder.append("\t          Total   RecordNum\t\t").append(recordNum).append("\n");
        builder.append("\t          Total   CommitNum\t\t").append(commitNum).append("\n");
        builder.append("\t          Total   SendNum\t\t").append(sendCount).append("\n");
        builder.append("\t          Total   Consume(ms)\t\t").append(System.currentTimeMillis() - start).append("\n");
        builder.append("\t          Handle  Consume(ms)\t\t").append(handleTime).append("\n");
        builder.append("\t          Build   Consume(ms)\t\t").append(buildTime).append("\n");
        builder.append("\t          Send    Consume(ms)\t\t").append(sendTime).append("\n");
        builder.append("\t          Commit  Consume(ms)\t\t").append(commitTime).append("\n");
        builder.append("\t       **************** end *****************\n");

        logger.info(builder.toString());
    }

    private void reset() {
        buildTime.set(0);
        sendTime.set(0);
        sendCount.set(0);
        handleTime.set(0);
        commitTime.set(0);
        recordNum.set(0);
        commitNum.set(0);
        start = System.currentTimeMillis();
    }
}
