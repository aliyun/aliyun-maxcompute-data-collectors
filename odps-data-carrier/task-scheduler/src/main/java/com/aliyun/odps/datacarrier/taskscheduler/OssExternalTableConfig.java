package com.aliyun.odps.datacarrier.taskscheduler;

public class OssExternalTableConfig extends ExternalTableConfig {
  private String endpoint;
  private String bucket;
  private String roleRan;

  public OssExternalTableConfig(String endpoint, String bucket, String roleRan, String location) {
    super(ExternalTableStorage.OSS, location);
    this.endpoint = endpoint;
    this.bucket = bucket;
    this.roleRan = roleRan;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getBucket() {
    return bucket;
  }

  public String getRoleRan() {
    return roleRan;
  }
}
