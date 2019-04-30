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
