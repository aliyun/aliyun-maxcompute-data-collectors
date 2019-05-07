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

package com.aliyun.odps.datacarrier.metaprocessor.report;

import htmlflow.HtmlView;
import java.util.HashMap;
import java.util.Map;
import org.xmlet.htmlapifaster.Body;
import org.xmlet.htmlapifaster.Div;
import org.xmlet.htmlapifaster.Html;
import org.xmlet.htmlapifaster.Table;

public class SummaryDivision {
  private class SummaryRecord {
    String hiveDatabaseName;
    int numberOfHighRisk = 0;
    int numberOfModerateRisk = 0;
  }

  private Map<String, SummaryRecord> summaryRecords = new HashMap<>();

  public void addHighRisk(String hiveDatabaseName) {
    SummaryRecord record = getOrCreateRecord(hiveDatabaseName);
    summaryRecords.get(hiveDatabaseName).numberOfHighRisk += 1;
  }

  public void addModerateRisk(String hiveDatabaseName) {
    SummaryRecord record = getOrCreateRecord(hiveDatabaseName);
    record.numberOfModerateRisk += 1;
  }

  private SummaryRecord getOrCreateRecord(String hiveDatabaseName) {
    if (!summaryRecords.containsKey(hiveDatabaseName)) {
      SummaryRecord record = new SummaryRecord();
      record.hiveDatabaseName = hiveDatabaseName;
      summaryRecords.put(hiveDatabaseName, record);
    }
    return summaryRecords.get(hiveDatabaseName);
  }

  public void addToBody(Body<Html<HtmlView>> body) {
    Table<Div<Body<Html<HtmlView>>>> table = body
        .div().attrId("summary")
        .h2().text("SUMMARY").__()
        .table();

    addTableHeader(table);

    for (String hiveDatabaseName : summaryRecords.keySet()) {
      SummaryRecord record = summaryRecords.get(hiveDatabaseName);
      addTableRecord(table, record);
    }
    table.__().__();
  }

  private void addTableHeader(Table<Div<Body<Html<HtmlView>>>> table) {
    table
        .tr()
          .th().text("HIVE DATABASE NAME").__()
          .th().text("# HIGH RISK").__()
          .th().text("# MODERATE RISK").__()
        .__();
  }

  private void addTableRecord(Table<Div<Body<Html<HtmlView>>>> table, SummaryRecord record) {
    table
        .tr()
          .td()
            .a().attrHref("#" + record.hiveDatabaseName).text(record.hiveDatabaseName).__()
          .__()
          .td().text(record.numberOfHighRisk).__()
          .td().text(record.numberOfModerateRisk).__()
        .__();
  }
}
