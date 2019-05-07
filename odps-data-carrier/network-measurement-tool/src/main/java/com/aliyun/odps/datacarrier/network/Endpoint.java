package com.aliyun.odps.datacarrier.network;

public class Endpoint {
  private String odpsEndpoint;
  private String tunnelEndpoint;
  private String location;

  public Endpoint(String odpsEndpoint, String tunnelEndpoint, String location) {
    if (odpsEndpoint == null || tunnelEndpoint == null) {
      throw new IllegalArgumentException("ODPS endpoint or tunnel endpoint cannot be null");
    }
    this.odpsEndpoint = odpsEndpoint;
    this.tunnelEndpoint = tunnelEndpoint;
    this.location = location;
  }

  public String getOdpsEndpoint() {
    return this.odpsEndpoint;
  }

  public String getTunnelEndpoint() {
    return this.tunnelEndpoint;
  }

  public String getLocation() {
    return this.location;
  }
}
