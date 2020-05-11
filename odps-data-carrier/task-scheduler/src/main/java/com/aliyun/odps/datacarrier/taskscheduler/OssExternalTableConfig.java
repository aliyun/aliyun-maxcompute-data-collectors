package com.aliyun.odps.datacarrier.taskscheduler;

public class OssExternalTableConfig extends ExternalTableConfig {

  // see Aliyun doc: https://help.aliyun.com/document_detail/66649.html?spm=a2c4g.11186623.6.785.b3466d036HETtT#title-y6w-tv9-sqg
  public enum DataFormat {
    CSV("com.aliyun.odps.CsvStorageHandler"),
    TSV("com.aliyun.odps.TsvStorageHandler");

    private String handler;

    DataFormat(String handlerClassName) {
      this.handler = handlerClassName;
    }

    public String getHandler() {
      return handler;
    }

    public static DataFormat getInstance(String format) {
      if (CSV.name().equals(format.toUpperCase())) {
        return CSV;
      } else if (TSV.name().equals(format.toUpperCase())) {
        return TSV;
      } else {
        throw new IllegalArgumentException("Unknown data format for oss storage: " + format);
      }
    }
  }

  private String endpoint;
  private String bucket;
  private String roleRan;
  private DataFormat dataFormat;

  public OssExternalTableConfig(String endpoint, String bucket, String roleRan) {
    this(endpoint, bucket, roleRan, DataFormat.CSV.name());
  }

  public OssExternalTableConfig(String endpoint, String bucket, String roleRan, String dataFormat) {
    super(ExternalTableStorage.OSS);
    this.endpoint = endpoint;
    this.bucket = bucket;
    this.roleRan = roleRan;
    this.dataFormat = DataFormat.getInstance(dataFormat);
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

  public DataFormat getDataFormat() {
    return dataFormat;
  }
}
