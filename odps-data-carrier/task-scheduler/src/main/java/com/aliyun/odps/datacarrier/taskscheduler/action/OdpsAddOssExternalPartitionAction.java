package com.aliyun.odps.datacarrier.taskscheduler.action;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.OssConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;
import com.aliyun.odps.datacarrier.taskscheduler.OssExternalTableConfig;

public class OdpsAddOssExternalPartitionAction extends OdpsSqlAction {

  private static final Logger LOG = LogManager.getLogger(OdpsAddOssExternalPartitionAction.class);


  public OdpsAddOssExternalPartitionAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    OssConfig ossConfig = MmaServerConfig.getInstance().getOssConfig();
    OssExternalTableConfig ossExternalTableConfig = new OssExternalTableConfig(
        ossConfig.getOssEndpoint(),
        ossConfig.getOssBucket(),
        ossConfig.getOssRoleArn(),
        null);

    LOG.info("OSS external table config: {}", GsonUtils.getFullConfigGson().toJson(ossExternalTableConfig));

    return OdpsSqlUtils.getAddPartitionStatement(
        actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return MmaServerConfig
        .getInstance()
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getDDLSettings();
  }
}
