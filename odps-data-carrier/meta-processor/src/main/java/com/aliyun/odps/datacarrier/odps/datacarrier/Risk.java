package com.aliyun.odps.datacarrier.odps.datacarrier;

public class Risk {

  public enum RISK_LEVEL {
    /**
     * There are risks that must be taken care of manually
     */
    HIGH,
    /**
     * There are risks, but can be solved automatically
     */
    MEDIUM,
    /**
     * No risk
     */
    LOW
  }

  private RISK_LEVEL riskLevel;
  private String description;

  public Risk(RISK_LEVEL riskLevel, String description) {
    if (riskLevel == null) {
      throw new IllegalArgumentException("Risk level cannot be null.");
    }
    this.riskLevel = riskLevel;
    this.description = description;
  }

  public RISK_LEVEL getRiskLevel() {
    return riskLevel;
  }

  public String getDescription() {
    return this.description;
  }

  public static Risk getUnsupportedTypeRisk(String originalType) {
    String description = "Unsupported type: " + originalType + ", have to be handled manually";
    return new Risk(RISK_LEVEL.HIGH, description);
  }

  public static Risk getInCompatibleTypeRisk(String originalType, String transformedType) {
    String description = "Incompatible type: " + originalType + ", can be transform to " +
        transformedType + " automatically, but may cause problem (e.g. precision loss)";
    return new Risk(RISK_LEVEL.MEDIUM, description);
  }

  public static Risk getNoRisk() {
    return new Risk(RISK_LEVEL.LOW, null);
  }
}
