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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;

public class JobProgressReporter {

  private static final Logger LOG = LogManager.getLogger(JobProgressReporter.class);

  private static final int MMA_CLIENT_PROGRESS_BAR_LENGTH = 20;
  private static final String[] PROGRESS_INDICATOR = new String[] {".  ", ".. ", "..."};
  private static final String PROGRESS_STR_FORMAT = "%-50s| RUNNING%s   %s   %.2f%%\n";

  private int numPrintedLines = 0;
  private int progressIndicatorIdx = 0;

  public JobProgressReporter() {
    if (!InPlaceUpdates.isUnixTerminal()) {
      System.err.println("Cannot report progress, please use a UNIX terminal");
    }
  }

  public void report(String jobName, MmaMetaManager.MigrationProgress migrationProgress) {
    if (!InPlaceUpdates.isUnixTerminal()) {
      return;
    }

    resetCursor();

    String line = String.join("", getProgressStr(jobName, migrationProgress));
    numPrintedLines = InPlaceUpdates.reprintMultiLine(System.err, line);
  }

  public void report(Map<String, MmaMetaManager.MigrationProgress> jobNameToMigrationProgress) {
    if (!InPlaceUpdates.isUnixTerminal()) {
      return;
    }

    resetCursor();

    List<String> lines = new LinkedList<>();
    for (Map.Entry<String, MmaMetaManager.MigrationProgress> entry :
        jobNameToMigrationProgress.entrySet()) {

      lines.add(getProgressStr(entry.getKey(), entry.getValue()));
    }

    lines.sort(String::compareToIgnoreCase);

    numPrintedLines = InPlaceUpdates.reprintMultiLine(System.err, String.join("", lines));
  }

  private void resetCursor() {
    LOG.info("Number of printed lines: {}", numPrintedLines);

    progressIndicatorIdx += 1;

    if (numPrintedLines > 0) {
      InPlaceUpdates.rePositionCursor(System.err, numPrintedLines);
      InPlaceUpdates.resetForward(System.err);
      numPrintedLines = 0;
    }
  }

  private String getProgressStr(String jobName, MmaMetaManager.MigrationProgress progress) {
    String curProgressIndicator =
        PROGRESS_INDICATOR[progressIndicatorIdx % PROGRESS_INDICATOR.length];

    float succeededPercent = 0;
    if (progress != null) {
      int numPartitions = progress.getNumPendingPartitions()
                          + progress.getNumRunningPartitions()
                          + progress.getNumFailedPartitions()
                          + progress.getNumSucceededPartitions();

      if (numPartitions == 0) {
        succeededPercent = 1;
      } else {
        succeededPercent = progress.getNumSucceededPartitions() / (float) numPartitions;
      }
    }

    StringBuilder progressBarBuilder = new StringBuilder("[");
    for (int i = 0; i < MMA_CLIENT_PROGRESS_BAR_LENGTH; i++) {
      if (i > succeededPercent * MMA_CLIENT_PROGRESS_BAR_LENGTH) {
        progressBarBuilder.append(" ");
      } else {
        progressBarBuilder.append("*");
      }
    }
    progressBarBuilder.append("]");


    return String.format(PROGRESS_STR_FORMAT,
                         jobName,
                         curProgressIndicator,
                         progressBarBuilder.toString(),
                         succeededPercent * 100);
  }
}
