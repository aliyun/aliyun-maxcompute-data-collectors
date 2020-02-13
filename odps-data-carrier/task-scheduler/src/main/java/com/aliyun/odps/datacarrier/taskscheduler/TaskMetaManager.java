package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static com.aliyun.odps.datacarrier.taskscheduler.Constants.FAILOVER_OUTPUT;

public class TaskMetaManager {

  private static final Logger LOG = LogManager.getLogger(TaskMetaManager.class);
  private static Path failoverFilePath;


  public TaskMetaManager () {
    this.failoverFilePath = Paths.get(System.getProperty("user.dir"), FAILOVER_OUTPUT);
  }

  public void writeToFailoverFile(String taskName) {
    OutputStream os = null;
    try {
      os = new FileOutputStream(new File(failoverFilePath.toString()), true);
      os.write(taskName.getBytes(), 0, taskName.length());
    } catch (IOException e) {
      e.printStackTrace();
    }finally{
      try {
        os.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void loadFailoverFile() {
    if (Files.notExists(this.failoverFilePath)) {
      LOG.info("Failover file is not found, skip failover.");
      return;
    }
    try {
      List<String> taskNames = Files.readAllLines(this.failoverFilePath);
//      finishedTasks.addAll(taskNames);
    } catch (Exception e) {
      LOG.error("Read failover file failed.");
    }
  }


  public List<TableMetaModel> getSucceededTableMeta() {
    //TODO: implement
    return Collections.emptyList();
  }

}
