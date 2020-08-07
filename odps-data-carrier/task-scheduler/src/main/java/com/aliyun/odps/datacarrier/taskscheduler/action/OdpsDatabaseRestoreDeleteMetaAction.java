package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.Constants;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OdpsDatabaseRestoreDeleteMetaAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsDatabaseRestoreDeleteMetaAction.class);

  private String uniqueId;
  private MmaMetaManager mmaMetaManager;

  public OdpsDatabaseRestoreDeleteMetaAction(String id, String uniqueId, MmaMetaManager mmaMetaManager) {
    super(id);
    this.uniqueId = uniqueId;
    this.mmaMetaManager = mmaMetaManager;
  }

  @Override
  public void doAction() throws MmaException {
    mmaMetaManager.removeRestoreJob(uniqueId);
    LOG.info("Action {}, remove information with unique id {} from {}", id, uniqueId, Constants.MMA_OBJ_RESTORE_TBL_NAME);
  }
}
