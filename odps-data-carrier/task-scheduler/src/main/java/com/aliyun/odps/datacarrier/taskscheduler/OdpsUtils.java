package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsFunctionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

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
      LOG.error("Get function {}.{} failed", databaseName, functionName, e);
    }
    return null;
  }

  public static void createFunction(String project, OdpsFunctionInfo functionInfo, boolean isUpdate) throws OdpsException {
    Function function = new Function();
      function.setName(functionInfo.getFunctionName());
      function.setClassPath(functionInfo.getClassName());
      function.setResources(functionInfo.getUseList());
      LOG.info("Create function {}.{} class {}, resources {}, update {}",
          project, functionInfo.getFunctionName(), functionInfo.getClassName(), functionInfo.getUseList(), isUpdate);
      if (odps.functions().exists(project, functionInfo.getFunctionName()) && isUpdate) {
        odps.functions().update(project, function);
      } else {
        odps.functions().create(project, function);
      }
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

  public static void addFileResource(String projectName, FileResource resource, String absoluteLocalFilePath, boolean isUpdate) throws OdpsException {
    File file = new File(absoluteLocalFilePath);
    if (file.exists()) {
      FileInputStream inputStream = null;
      try {
        inputStream = new FileInputStream(file);
        if (isUpdate) {
          try {
            odps.resources().update(projectName, resource, inputStream);
          } catch (NoSuchObjectException e) {
            inputStream.close();
            inputStream = new FileInputStream(file);
            odps.resources().create(projectName, resource, inputStream);
          }
        } else {
          odps.resources().create(projectName, resource, inputStream);
        }
      } catch (IOException e) {
        throw new OdpsException("Add resource " + resource.getName() + " to " + projectName + " failed cause upload file fail.", e);
      } finally {
        if (inputStream != null) {
          try {
            inputStream.close();
          } catch (IOException e) {
          }
        }
      }
    } else {
      throw new OdpsException("File not found:" + file.getAbsolutePath() + " when add resource " + resource.getName() + " to " + projectName);
    }
  }

  public static void addTableResource(String projectName, TableResource resource, boolean isUpdate) throws OdpsException {
    if (!isUpdate) {
      odps.resources().create(projectName, resource);
    } else {
      try {
        odps.resources().update(projectName, resource);
      } catch (NoSuchObjectException e) {
        odps.resources().create(projectName, resource);
      }
    }
  }
}
