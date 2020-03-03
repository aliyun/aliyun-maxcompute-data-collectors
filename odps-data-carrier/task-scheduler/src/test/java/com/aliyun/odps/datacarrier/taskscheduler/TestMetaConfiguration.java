package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import java.io.File;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.META_CONFIG_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMetaConfiguration {
  private static final Logger LOG = LogManager.getLogger(TestMetaConfiguration.class);

  @Test (timeout = 5000)
  public void testReadConfigFile() throws Exception {
    String currentDir = System.getProperty("user.dir");
    File configFile = new File(currentDir + "/src/test/resources/", META_CONFIG_FILE);
    MetaConfigurationUtils.generateConfigFile(configFile, null, null);
    MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
    assertTrue(metaConfiguration.validateAndInitConfig());
    configFile.delete();
  }

  @Test (timeout = 5000)
  public void testGenerateMetaConfigurationFromTableMapping() throws Exception {
    String currentDir = System.getProperty("user.dir");
    File configFile = new File(currentDir + "/src/test/resources/", META_CONFIG_FILE);
    String tableMappingFilePathStr = "/src/test/resources/dma_demo.txt";
    MetaConfigurationUtils.generateConfigFile(configFile, tableMappingFilePathStr, null);
    MetaConfiguration metaConfiguration = MetaConfigurationUtils.readConfigFile(configFile);
    assertTrue(metaConfiguration.validateAndInitConfig());
    assertEquals(metaConfiguration.getTableGroups().size(), 4);
    assertEquals(metaConfiguration.getTableGroups().get(0).getTableConfigs().size(), 5);
    assertEquals(metaConfiguration.getTableGroups().get(1).getTableConfigs().size(), 1);
    assertEquals(metaConfiguration.getTableGroups().get(2).getTableConfigs().size(), 1);
    assertEquals(metaConfiguration.getTableGroups().get(3).getTableConfigs().size(), 2);
    configFile.delete();
  }
}
