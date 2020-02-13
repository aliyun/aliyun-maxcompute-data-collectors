package com.aliyun.odps.datacarrier.metacarrier;

import com.aliyun.odps.datacarrier.commons.MetaManager.TableMetaModel;
import com.aliyun.odps.datacarrier.commons.MetaManager.TablePartitionMetaModel;
import org.apache.thrift.TException;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface MetaCarrier {

  TableMetaModel getTableMeta(String databaseName, String tableName) throws TException;

  TablePartitionMetaModel getTablePartitionMeta(String databaseName,
                                                String tableName,
                                                int numOfPartitions,
                                                List<Map<String, String>> partitionSpecs)
      throws TException;


  void carry(MetaCarrierConfiguration configuration) throws IOException, TException;

  /**
   * Generate TableMetaModel/TablePartitionMetaModel filtering out succeeded tables and partitions
   * according to failoverConfig.
   * @param configuration
   * @param failoverConfig
   * @throws IOException
   * @throws TException
   */
  void generateMetaModelWithFailover(MetaCarrierConfiguration configuration, Configuration failoverConfig)
      throws IOException, TException;

  /**
   * @param databaseName
   * @return Non-partition tables need to migrate data.
   * @throws TException
   */
  TableMetaModel getTables(String databaseName) throws TException;

  /**
   *
   * @param databaseName
   * @param tableName
   * @return Partition table left partitions need to migrate data.
   * @throws TException
   */
  TablePartitionMetaModel getPartitionMeta(String databaseName, String tableName) throws TException;

}
