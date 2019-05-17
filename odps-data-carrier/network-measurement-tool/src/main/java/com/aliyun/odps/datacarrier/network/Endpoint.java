package com.aliyun.odps.datacarrier.network;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.datacarrier.network.Endpoints.LOCATION;
import com.aliyun.odps.datacarrier.network.Endpoints.NETWORK;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Endpoint {
  private String odpsEndpoint;
  private String tunnelEndpoint;
  private LOCATION location;
  private NETWORK network;

  public Endpoint(String odpsEndpoint, String tunnelEndpoint, LOCATION location, NETWORK network) {
    if (odpsEndpoint == null) {
      throw new IllegalArgumentException("ODPS endpoint cannot be null");
    }
    this.odpsEndpoint = odpsEndpoint;
    this.tunnelEndpoint = tunnelEndpoint;
    this.location = location;
    this.network = network;
  }

  public String getOdpsEndpoint() {
    return this.odpsEndpoint;
  }

  public String getTunnelEndpoint() {
    return this.tunnelEndpoint;
  }

  public LOCATION getLocation() {
    return this.location;
  }

  public NETWORK getNetwork() {
    return this.network;
  }

  @Override
  public String toString() {
    return network + "-" + location + ": " + odpsEndpoint;
  }
}
