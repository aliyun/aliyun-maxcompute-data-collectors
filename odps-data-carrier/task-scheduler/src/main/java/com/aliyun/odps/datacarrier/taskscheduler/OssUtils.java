package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_OBJECT_ROOT_FOLDER;

public class OssUtils {
  private static final Logger LOG = LogManager.getLogger(OssUtils.class);

  public static void createFile(String fileName, // relative path from bucket, such as a/b/c.txt
                                String content) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    LOG.info("Create oss file: {}, endpoint: {}, bucket: {}",
       fileName, ossConfig.getOssLocalEndpoint(), ossConfig.getOssBucket());
    OSS ossClient = createOssClient();
    PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getOssBucket(), fileName, new ByteArrayInputStream(content.getBytes()));
    ossClient.putObject(putObjectRequest);
    ossClient.shutdown();
  }

  public static void createFile(String fileName, // relative path from bucket, such as a/b/c.txt
                                InputStream inputStream) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    PutObjectRequest putObjectRequest = new PutObjectRequest(ossConfig.getOssBucket(), fileName, inputStream);
    ossClient.putObject(putObjectRequest);
    ossClient.shutdown();
  }

  public static String readFile(String fileName) throws IOException {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    OSSObject ossObject = ossClient.getObject(ossConfig.getOssBucket(), fileName);
    StringBuilder builder = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
    while (true) {
      String line = reader.readLine();
      if (line == null)
        break;
      builder.append(line).append('\n');
    }
    reader.close();
    ossClient.shutdown();
    return builder.toString();
  }

  public static boolean exists(String fileName) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    boolean exists = ossClient.doesObjectExist(ossConfig.getOssBucket(), fileName);
    ossClient.shutdown();
    return exists;
  }

  public static String downloadFile(String fileName) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    File localFile = new File("oss_files/" + fileName);
    if(!localFile.getParentFile().exists()){
      localFile.getParentFile().mkdirs();
    }
    ossClient.getObject(new GetObjectRequest(ossConfig.getOssBucket(), fileName), localFile);
    ossClient.shutdown();
    LOG.info("Download oss file {} to local {}", fileName, localFile.getAbsolutePath());
    return localFile.getAbsolutePath();
  }

  public static List<String> listBucket(String prefix) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    ArrayList<String> allFiles = new ArrayList<>();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossConfig.getOssBucket());
    listObjectsRequest.setMaxKeys(1000);
    listObjectsRequest.setPrefix(prefix);
    listObjectsRequest.setDelimiter("/");
    while (true) {
      ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
      List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
      for (OSSObjectSummary s : sums) {
        allFiles.add(s.getKey());
      }
      if (!objectListing.isTruncated()) {
        break;
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    }
    return allFiles;
  }

  public static List<String> listBucket(String prefix, String delimiter) {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OSS ossClient = createOssClient();
    ArrayList<String> allFiles = new ArrayList<>();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest(ossConfig.getOssBucket());
    listObjectsRequest.setMaxKeys(1000);
    listObjectsRequest.setPrefix(prefix);
    listObjectsRequest.setDelimiter(delimiter);
    while (true) {
      ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
      for(String fullName : objectListing.getCommonPrefixes()) {
        String[] splitResult = fullName.split(delimiter);
        allFiles.add(splitResult[splitResult.length - 1]);
      }
      if (!objectListing.isTruncated()) {
        break;
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    }
    return allFiles;
  }

  public static String getOssPathToExportObject(String taskName,
                                                String folderName,
                                                String database,
                                                String objectName,
                                                String ossFileName) {
    StringBuilder builder = new StringBuilder();
    builder.append(getFolderNameWithSeparator(EXPORT_OBJECT_ROOT_FOLDER))
        .append(getFolderNameWithSeparator(taskName))
        .append(getFolderNameWithSeparator(folderName))
        .append(getFolderNameWithSeparator(database.toLowerCase()))
        .append(getFolderNameWithSeparator(objectName.toLowerCase()))
        .append(ossFileName);
    return builder.toString();
  }

  public static String getOssFolderToExportObject(String taskName,
                                                  String folderName,
                                                  String database) {
    StringBuilder builder = new StringBuilder();
    builder.append(getFolderNameWithSeparator(EXPORT_OBJECT_ROOT_FOLDER))
        .append(getFolderNameWithSeparator(taskName))
        .append(getFolderNameWithSeparator(folderName))
        .append(getFolderNameWithSeparator(database.toLowerCase()));
    return builder.toString();
  }

  private static OSS createOssClient() {
    MmaConfig.OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    return new OSSClientBuilder().build(ossConfig.getOssLocalEndpoint(), ossConfig.getOssAccessId(), ossConfig.getOssAccessKey());
  }

  private static String getFolderNameWithSeparator(String folderName) {
    if (folderName.endsWith("/")) {
      return folderName;
    }
    return folderName + "/";
  }
}
