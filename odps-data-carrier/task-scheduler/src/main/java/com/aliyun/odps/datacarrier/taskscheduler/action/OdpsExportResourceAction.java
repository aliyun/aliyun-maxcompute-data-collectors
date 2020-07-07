package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_DDL_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_RESOURCE_DDL_FOLDER_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_RESOURCE_OBJECT_FOLDER_NAME;

public class OdpsExportResourceAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsExportResourceAction.class);

  private Resource resource;

  public OdpsExportResourceAction(String id, Resource resource) {
    super(id);
    this.resource = resource;
  }

   @Override
  public void doAction() throws MmaException {
    try {
      setProgress(ActionProgress.RUNNING);
      if (StringUtils.isEmpty(resource.getName())) {
        LOG.error("Invalid resource name {} for task {}", resource.getName(), id);
        setProgress(ActionProgress.FAILED);
        return;
      }
      // 要注意文件名和alias关系, file和ARCHIVE的情况是用户可以指定alias名称的
      String fileName = resource.getName();
      String filePath = "${localPath}/resources/" + fileName;
      Resource.Type type = resource.getType();
      StringBuilder builder = new StringBuilder();
      switch (type) {
        case FILE:
        case ARCHIVE:
          builder.append(getFilePathWithSuffix(filePath, fileName));
          break;
        case PY:
        case JAR:
          builder.append(filePath);
          break;
        case TABLE:
        {
          TableResource tableResource = (TableResource) resource;
          builder.append(tableResource.getSourceTable().getName());
          PartitionSpec partitionSpec = tableResource.getSourceTablePartition();
          if (partitionSpec != null) {
            builder.append(" partition(").append(partitionSpec.toString()).append(")");
          }
          break;
        }
        default:
          LOG.error("Invalid resource type {} in task {}", type, id);
          throw new MmaException("Invalid resource type: " + type.toString() + " in " + id);
      }
      String script = OdpsSqlUtils.getAddResourceStatement(type.name(), builder.toString(), fileName, resource.getComment());
      String ossFileName = OssUtils.getOssPathToExportObject(EXPORT_RESOURCE_DDL_FOLDER_NAME,
          resource.getProject(),
          fileName,
          EXPORT_DDL_FILE_NAME);
      OssUtils.createFile(ossFileName, script);
      if (!Resource.Type.TABLE.equals(type)) {
        FileResource fileResource = (FileResource) resource;
        InputStream inputStream = OdpsUtils.getInstance().resources()
            .getResourceAsStream(fileResource.getProject(), fileResource.getName());
        ossFileName = OssUtils.getOssPathToExportObject(EXPORT_RESOURCE_OBJECT_FOLDER_NAME,
            resource.getProject(),
            fileName,
            fileName);
        OssUtils.createFile(ossFileName, inputStream);
      }
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id, ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  private String getFilePathWithSuffix(String filePath, String fileName) {
    if (fileName != null && !fileName.trim().equals("")) {
      int index1 = fileName.lastIndexOf("/");
      int index2 = fileName.lastIndexOf("\\");
      // file的情况把alias和文件名作为后缀
      filePath = filePath + "_" + fileName.substring((Math.max(index1, index2)) + 1);
    }
    return filePath;
  }
}