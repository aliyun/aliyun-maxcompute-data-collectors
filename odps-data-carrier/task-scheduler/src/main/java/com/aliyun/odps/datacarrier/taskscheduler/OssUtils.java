package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PutObjectRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.OSS_ROOT_FOLDER_NAME;

public class OssUtils {
  private static final Logger LOG = LogManager.getLogger(OssUtils.class);

  public static void createFile(String fileName, // relative path from bucket, such as a/b/c.txt
                                String content) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    String endpoint = ossConfig.getOssLocalEndpoint();
    LOG.info("Create oss file {}, endpoint: {}, bucket: {}",
       fileName, endpoint, ossConfig.getOssBucket());
    OSS ossClient = new OSSClientBuilder().build(endpoint, ossConfig.getOssAccessId(), ossConfig.getOssAccessKey());
    PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getOssBucket(), fileName, new ByteArrayInputStream(content.getBytes()));
    ossClient.putObject(putObjectRequest);
    ossClient.shutdown();
  }

  public static void createFile(String fileName, // relative path from bucket, such as a/b/c.txt
                                InputStream inputStream) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    String endpoint = ossConfig.getOssLocalEndpoint();
    LOG.info("Create oss file {}, endpoint: {}, bucket: {}",
       fileName, endpoint, ossConfig.getOssBucket());
    OSS ossClient = new OSSClientBuilder().build(endpoint, ossConfig.getOssAccessId(), ossConfig.getOssAccessKey());
    PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getOssBucket(), fileName, inputStream);
    ossClient.putObject(putObjectRequest);
    ossClient.shutdown();
  }

  public static String getOssPathToExportObject(String folderName,
                                                String database,
                                                String objectName,
                                                String ossFileName) {
    StringBuilder builder = new StringBuilder();
    builder.append(OSS_ROOT_FOLDER_NAME).append(folderName)
        .append(database).append(".db").append("/")
        .append(objectName).append("/")
        .append(System.currentTimeMillis()).append("/")
        .append(ossFileName);
    return builder.toString();
  }
}
