package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;
import com.aliyun.odps.utils.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_OBJECT_FILE_NAME;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_RESOURCE_FOLDER;

public class OdpsRestoreResourceAction extends OdpsRestoreAction {
  private static final Logger LOG = LogManager.getLogger(OdpsRestoreResourceAction.class);

  public OdpsRestoreResourceAction(String id, MmaConfig.ObjectRestoreConfig restoreConfig) {
    super(id, restoreConfig);
  }

  @Override
  public void restore() throws Exception {
    String metaFileName = getRestoredFilePath(EXPORT_RESOURCE_FOLDER, EXPORT_META_FILE_NAME);
    String content = OssUtils.readFile(metaFileName);
    OdpsResourceInfo resourceInfo = GsonUtils.getFullConfigGson().fromJson(content, OdpsResourceInfo.class);
    Resource resource = null;
    switch (resourceInfo.getType()) {
      case ARCHIVE:
        resource = new ArchiveResource();
        break;
      case PY:
        resource = new PyResource();
        break;
      case JAR:
        resource = new JarResource();
        break;
      case FILE:
        resource = new FileResource();
        break;
      case TABLE:
        PartitionSpec spec = null;
        String partitionSpec = resourceInfo.getPartitionSpec();
        if (!StringUtils.isNullOrEmpty(partitionSpec)) {
          spec = new PartitionSpec(partitionSpec);
        }
        resource = new TableResource(resourceInfo.getTableName(), null, spec);
        break;
    }
    resource.setName(resourceInfo.getAlias());
    resource.setComment(resourceInfo.getComment());
    if (Resource.Type.TABLE.equals(resourceInfo.getType())) {
      OdpsUtils.addTableResource(getDestinationProject(), (TableResource) resource, isUpdate());
    } else {
      String fileName = getRestoredFilePath(EXPORT_RESOURCE_FOLDER, EXPORT_OBJECT_FILE_NAME);
      String localFilePath = OssUtils.downloadFile(fileName);
      OdpsUtils.addFileResource(getDestinationProject(), (FileResource) resource, localFilePath, isUpdate());
    }
    LOG.info("Restore resource {} succeed", content);
  }
}
