package com.aliyun.odps.datacarrier.odps.datacarrier.report;

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
                 .attrHref("style.css")
          .__()
        .__();

    Body<Html<HtmlView>> body = html.body();
    body
        .h1().text("ODPS compatibility report").__();
    summaryDivision.addToBody(body);
    databaseDivision.addToBody(body);
    tableDivision.addToBody(body);
    return body.__().__().render();
  }
}
