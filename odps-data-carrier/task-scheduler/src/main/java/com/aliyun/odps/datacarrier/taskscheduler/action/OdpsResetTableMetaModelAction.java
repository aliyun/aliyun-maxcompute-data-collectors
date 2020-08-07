package com.aliyun.odps.datacarrier.taskscheduler.action;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSourceFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OdpsResetTableMetaModelAction extends OdpsNoSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsResetTableMetaModelAction.class);

  private String sourceDatabase;
  private String sourceTable;
  private String destinationDatabase;
  private String destinationTable;

  public OdpsResetTableMetaModelAction(String id,
                                       String sourceDatabase,
                                       String sourceTable,
                                       String destinationDatabase,
                                       String destinationTable) {
    super(id);
    this.sourceDatabase = sourceDatabase;
    this.sourceTable = sourceTable;
    this.destinationDatabase = destinationDatabase;
    this.destinationTable = destinationTable;
  }

  @Override
  public void doAction() throws MmaException {
    try {
      MetaSource metaSource = MetaSourceFactory.getMetaSource();
      MetaSource.TableMetaModel tableMetaModel = metaSource.getTableMeta(sourceDatabase, sourceTable);
      LOG.info("GetTableMeta {}.{}, partitions {}", sourceDatabase, sourceTable, tableMetaModel.partitions.size());
      MmaConfig.TableMigrationConfig config = new MmaConfig.TableMigrationConfig(
          sourceDatabase,
          sourceTable,
          destinationDatabase,
          destinationTable,
          null);
      config.apply(tableMetaModel);
      actionExecutionContext.setTableMetaModel(tableMetaModel);
      LOG.info("Reset table meta model for {}, source {}.{}, destination {}.{}, partition size {}",
          id, tableMetaModel.databaseName, tableMetaModel.tableName,
          tableMetaModel.odpsProjectName, tableMetaModel.odpsTableName,
          tableMetaModel.partitions.size());
    } catch (Exception e) {
      LOG.error("Exception when reset table meta for task {}, table {}.{}, destination {}.{}",
          id, sourceDatabase, sourceTable, destinationDatabase, destinationTable, e);
      throw new MmaException("Reset table meta fail for " + id, e);
    }
  }
}
