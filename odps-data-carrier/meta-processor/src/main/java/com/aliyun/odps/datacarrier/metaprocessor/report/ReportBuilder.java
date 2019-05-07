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

import com.aliyun.odps.datacarrier.commons.GeneratedStatement;
import com.aliyun.odps.datacarrier.commons.risk.Risk;
import com.aliyun.odps.datacarrier.commons.risk.Risk.RISK_LEVEL;
import htmlflow.HtmlView;
import htmlflow.StaticHtml;
import org.xmlet.htmlapifaster.Body;
import org.xmlet.htmlapifaster.EnumRelType;
import org.xmlet.htmlapifaster.EnumTypeContentType;
import org.xmlet.htmlapifaster.Html;


// TODO: order the databases & tables
public class ReportBuilder {
  private SummaryDivision summaryDivision;
  private HiveDatabaseDivision databaseDivision;
  private HiveTableDivision tableDivision;

  public ReportBuilder() {
    this.summaryDivision = new SummaryDivision();
    this.databaseDivision = new HiveDatabaseDivision();
    this.tableDivision = new HiveTableDivision();
  }

  public void add(String hiveDatabaseName, String hiveTableName, GeneratedStatement statement) {
    for (Risk risk : statement.getRisks()) {
      if (risk.getRiskLevel().equals(RISK_LEVEL.HIGH)) {
        summaryDivision.addHighRisk(hiveDatabaseName);
        databaseDivision.addHighRisk(hiveDatabaseName, hiveTableName);
        tableDivision.addRisk(hiveDatabaseName, hiveTableName, risk);
      } else if (risk.getRiskLevel().equals(RISK_LEVEL.MODERATE)) {
        summaryDivision.addModerateRisk(hiveDatabaseName);
        databaseDivision.addModerateRisk(hiveDatabaseName, hiveTableName);
        tableDivision.addRisk(hiveDatabaseName, hiveTableName, risk);
      }
    }
  }

  public String build() {
    Html<HtmlView> html = StaticHtml.view().html();

    // Handle css
    html
        .head()
          .link().attrRel(EnumRelType.STYLESHEET)
                 .attrType(EnumTypeContentType.TEXT_CSS)
                 .attrHref("res/style.css")
          .__()
        .__();

    Body<Html<HtmlView>> body = html.body();
    body
        .h1().text("MaxCompute compatibility report").__();
    summaryDivision.addToBody(body);
    databaseDivision.addToBody(body);
    tableDivision.addToBody(body);
    return body.__().__().render();
  }
}
