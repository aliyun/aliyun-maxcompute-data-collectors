package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AddMigrationJobAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(AddMigrationJobAction.class);

  private MmaConfig.TableMigrationConfig config;
  private MmaMetaManager mmaMetaManager;

  public AddMigrationJobAction(String id,
                               MmaConfig.TableMigrationConfig config,
                               MmaMetaManager mmaMetaManager) {
    super(id);
    this.config = config;
    this.mmaMetaManager = mmaMetaManager;
  }

  @Override
  public void doAction() throws MmaException {
    try {
      mmaMetaManager.addMigrationJob(config);
      LOG.info("Add migration job {}", MmaConfig.TableMigrationConfig.toJson(config));
    } catch (Exception e) {
      LOG.error("Action {} Exception when create table migration job, config {}",
          id, MmaConfig.TableMigrationConfig.toJson(config), e);
      throw new MmaException("Create table migration task for " + id, e);
    }
  }
}
