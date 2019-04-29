package com.aliyun.odps.datacarrier.commons;

import com.aliyun.odps.datacarrier.commons.Risk;
import com.aliyun.odps.datacarrier.commons.Risk.RISK_LEVEL;
import java.util.ArrayList;
import java.util.List;

public class GeneratedStatement {
  private String statement;
  private RISK_LEVEL riskLevel = RISK_LEVEL.LOW;
  private List<Risk> risks = new ArrayList<>();

  public void setRisk(Risk risk) {
    if (!risk.getRiskLevel().equals(RISK_LEVEL.LOW)) {
      this.risks.add(risk);
    }
    if (risk.getRiskLevel().ordinal() < this.riskLevel.ordinal()) {
      this.riskLevel = risk.getRiskLevel();
    }
  }

  public List<Risk> getRisks() {
    return this.risks;
  }

  public RISK_LEVEL getRiskLevel() {
    return this.riskLevel;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public String getStatement() {
    return this.statement;
  }
}
