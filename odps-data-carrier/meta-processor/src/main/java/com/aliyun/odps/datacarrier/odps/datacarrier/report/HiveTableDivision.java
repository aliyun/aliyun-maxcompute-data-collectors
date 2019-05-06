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

package com.aliyun.odps.datacarrier.odps.datacarrier.report;

import com.aliyun.odps.datacarrier.commons.risk.Risk;
import htmlflow.HtmlView;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.xmlet.htmlapifaster.Body;
import org.xmlet.htmlapifaster.Div;
import org.xmlet.htmlapifaster.Html;
import org.xmlet.htmlapifaster.Table;

public class HiveTableDivision {

  private Map<String, Map<String, List<Risk>>> hiveTableRecords = new HashMap<>();

  public void addRisk(String hiveDatabaseName, String hiveTableName, Risk risk) {
    if (!hiveTableRecords.containsKey(hiveDatabaseName)) {
      hiveTableRecords.put(hiveDatabaseName, new HashMap<>());
    }
    if (!hiveTableRecords.get(hiveDatabaseName).containsKey(hiveTableName)) {
      hiveTableRecords.get(hiveDatabaseName).put(hiveTableName, new ArrayList<>());
    }
    hiveTableRecords.get(hiveDatabaseName).get(hiveTableName).add(risk);
  }

  public void addToBody(Body<Html<HtmlView>> body) {
    for (String hiveDatabaseName : hiveTableRecords.keySet()) {
      for (String hiveTableName : hiveTableRecords.get(hiveDatabaseName).keySet()) {
        List<Risk> risks = hiveTableRecords.get(hiveDatabaseName).get(hiveTableName);
        Table<Div<Body<Html<HtmlView>>>> table = body
            .div().attrId(hiveDatabaseName + "." + hiveTableName)
            .h2().text(hiveDatabaseName.toUpperCase() + "." + hiveTableName.toUpperCase()).__()
            .table();

        addTableHeader(table);

        for (Risk risk : risks) {
          addTableRisk(table, risk);
        }
        table.__().__();
      }
    }
  }

  private void addTableHeader(Table<Div<Body<Html<HtmlView>>>> table) {
    table
        .tr()
          .th().text("RISK LEVEL").__()
          .th().text("DESCRIPTION").__()
        .__();
  }

  private void addTableRisk(Table<Div<Body<Html<HtmlView>>>> table, Risk risk) {
    table
        .tr().attrClass(risk.getRiskLevel().toString())
          .td().text(risk.getRiskLevel()).__()
          .td().text(risk.getDescription()).__()
        .__();
  }
}
