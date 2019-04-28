package com.aliyun.odps.datacarrier.transfer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class OdpsConfig {
  private static final String ACCESS_ID = "access_id";
  private static final String ACCESS_KEY = "access_key";
  private static final String ODPS_ENDPOINT = "odps_endpoint";
  private static final String TUNNEL_ENDPOINT = "tunnel_endpoint";
  private static final String PROJECT_NAME = "project_name";

  private Properties properties;

  public OdpsConfig(String fileName) throws IOException {
    this.properties = new Properties();
    this.properties.load(new FileInputStream(fileName));
  }

  public String getAccessId() {
    return this.properties.getProperty(ACCESS_ID);
  }

  public String getAccessKey() {
    return this.properties.getProperty(ACCESS_KEY);
  }

  public String getOdpsEndpoint() {
    return this.properties.getProperty(ODPS_ENDPOINT);
  }

  public String getTunnelEndpoint() {
    return this.properties.getProperty(TUNNEL_ENDPOINT);
  }

  public String getProjectName() {
    return this.properties.getProperty(PROJECT_NAME);
  }
}
