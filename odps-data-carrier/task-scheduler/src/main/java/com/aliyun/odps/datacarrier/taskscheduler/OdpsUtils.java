package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.AliyunAccount;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OdpsUtils {
  private static final Logger LOG = LogManager.getLogger(OdpsUtils.class);

  private static Odps odps;

  static {
    MmaConfig.OdpsConfig odpsConfig = MmaServerConfig.getInstance().getOdpsConfig();
    odps = new Odps(new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey()));
    odps.setEndpoint(odpsConfig.getEndpoint());
    odps.setDefaultProject(odpsConfig.getProjectName());
  }

  public static Odps getInstance() {
    return odps;
  }

  public static Table getTable(String databaseName, String tableName) {
    try {
      if (odps.tables().exists(databaseName, tableName)) {
        return odps.tables().get(databaseName, tableName);
      }
    } catch (OdpsException e) {
      LOG.info("Get table {}.{} failed", databaseName, tableName, e);
    }
    return null;
  }

  public static Function getFunction(String databaseName, String functionName) {
    try {
      if (odps.functions().exists(databaseName, functionName)) {
        return odps.functions().get(databaseName, functionName);
      }
    } catch (OdpsException e) {
      LOG.info("Get function {}.{} failed", databaseName, functionName, e);
    }
    return null;
  }

  public static Resource getResource(String databaseName, String resourceName) {
    try {
      if (odps.resources().exists(databaseName, resourceName)) {
        return odps.resources().get(databaseName, resourceName);
      }
    } catch (OdpsException e) {
      LOG.info("Get resource {}.{} failed", databaseName, resourceName, e);
    }
    return null;
  }
}
