package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.META_CONFIG_FILE;
import static com.aliyun.odps.datacarrier.taskscheduler.Constants.ODPS_DATA_CARRIER;
import static org.junit.Assert.assertTrue;

public class TestMetaConfiguration {
  private static final Logger LOG = LogManager.getLogger(TestMetaConfiguration.class);

  @Test (timeout = 5000)
  public void testReadConfigFile() throws Exception {
    String currentDir = System.getProperty("user.dir");
    File configFile = new File(currentDir + "/src/test/resources/", META_CONFIG_FILE);
    LOG.info("configFile = {}", configFile.toString());

    MetaConfigurationUtils.generateConfigFile(configFile);
    MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
    assertTrue(metaConfiguration.validateAndInitConfig());
  }
}
