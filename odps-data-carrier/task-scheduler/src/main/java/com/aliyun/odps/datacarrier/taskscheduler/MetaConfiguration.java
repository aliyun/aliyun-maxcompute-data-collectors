package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.utils.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MetaConfiguration {
  private static final Logger LOG = LogManager.getLogger(MetaConfiguration.class);

  private String user;
  private String migrationJobName;
  private volatile String migrationJobId;

  //basic environment configuration
  private DataSource dataSource;
  private OssDataSource ossDataSource;
  private HiveMetaSource hiveMetaSource;
  private OdpsConfiguration odpsConfiguration;
  private List<TablesGroup> tablesGroupList;

  //global config for all tables.
  private Config globalTableConfig;

  private volatile Map<String, Map<String, Table>> tableConfig = new HashMap<>();

  public MetaConfiguration(String user,
                           String migrationJobName,
                           DataSource dataSource) {
    this.user = user;
    this.migrationJobName = migrationJobName;
    this.migrationJobId = createMigrationJobId();
    this.dataSource = dataSource;
  }

  public String createMigrationJobId() {
    return this.user + "_" + this.migrationJobName + "_" + UUID.randomUUID();
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getMigrationJobName() {
    return migrationJobName;
  }

  public void setMigrationJobName(String migrationJobName) {
    this.migrationJobName = migrationJobName;
  }

  public String getMigrationJobId() {
    return migrationJobId;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public OssDataSource getOssDataSource() {
    return ossDataSource;
  }

  public void setOssDataSource(OssDataSource ossDataSource) {
    this.ossDataSource = ossDataSource;
  }

  public HiveMetaSource getHiveMetaSource() {
    return hiveMetaSource;
  }

  public void setHiveMetaSource(HiveMetaSource hiveMetaSource) {
    this.hiveMetaSource = hiveMetaSource;
  }

  public OdpsConfiguration getOdpsConfiguration() {
    return odpsConfiguration;
  }

  public void setOdpsConfiguration(OdpsConfiguration odpsConfiguration) {
    this.odpsConfiguration = odpsConfiguration;
  }

  public List<TablesGroup> getTablesGroupList() {
    return tablesGroupList;
  }

  public void setTablesGroupList(List<TablesGroup> tablesGroupList) {
    this.tablesGroupList = tablesGroupList;
  }

  public Config getGlobalTableConfig() {
    return globalTableConfig;
  }

  public void setGlobalTableConfig(Config globalTableConfig) {
    this.globalTableConfig = globalTableConfig;
  }

  public static class OssDataSource {
    private String ossEndpoint;
    private String ossBucket;

    public OssDataSource(String ossEndpoint, String ossBucket) {
      this.ossEndpoint = ossEndpoint;
      this.ossBucket = ossBucket;
    }

    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(ossEndpoint) &&
          !StringUtils.isNullOrEmpty(ossBucket));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OssDataSource{");
      sb.append("ossEndpoint='").append(ossEndpoint).append('\'');
      sb.append(", ossBucket='").append(ossBucket).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  public static class HiveMetaSource {
    private String hiveJdbcAddress;
    private String thriftAddr;
    private String krbPrincipal;
    private String keyTab;
    private String[] krbSystemProperties;

    public HiveMetaSource(String hiveJdbcAddress,
                          String thriftAddr,
                          String krbPrincipal,
                          String keyTab,
                          String[] krbSystemProperties) {
      this.hiveJdbcAddress = hiveJdbcAddress;
      this.thriftAddr = thriftAddr;
      this.krbPrincipal = krbPrincipal;
      this.keyTab = keyTab;
      this.krbSystemProperties = krbSystemProperties;
    }

    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(hiveJdbcAddress) &&
          !StringUtils.isNullOrEmpty(thriftAddr));
    }

    public String getHiveJdbcAddress() {
      return hiveJdbcAddress;
    }

    public String getThriftAddr() {
      return thriftAddr;
    }

    public String getKrbPrincipal() {
      return krbPrincipal;
    }

    public String getKeyTab() {
      return keyTab;
    }

    public String[] getKrbSystemProperties() {
      return krbSystemProperties;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("HiveMetaSource{");
      sb.append("hiveJdbcAddress='").append(hiveJdbcAddress).append('\'');
      sb.append(", thriftAddr='").append(thriftAddr).append('\'');
      sb.append(", krbPrincipal='").append(krbPrincipal).append('\'');
      sb.append(", keyTab='").append(keyTab).append('\'');
      sb.append(", krbSystemProperties=").append(Arrays.toString(krbSystemProperties));
      sb.append('}');
      return sb.toString();
    }
  }

  public static class OdpsConfiguration {
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String projectName;
    private String tunnelEndpoint;

    public OdpsConfiguration(String accessId,
                             String accessKey,
                             String endpoint,
                             String projectName,
                             String tunnelEndpoint) {
      this.accessId = accessId;
      this.accessKey = accessKey;
      this.endpoint = endpoint;
      this.projectName = projectName;
      this.tunnelEndpoint = tunnelEndpoint;
    }

    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(accessId) &&
          !StringUtils.isNullOrEmpty(accessKey) &&
          !StringUtils.isNullOrEmpty(endpoint) &&
          !StringUtils.isNullOrEmpty(projectName) &&
          !StringUtils.isNullOrEmpty(tunnelEndpoint));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OdpsConfiguration{");
      sb.append("accessId='").append(accessId).append('\'');
      sb.append(", accessKey='").append(accessKey).append('\'');
      sb.append(", endpoint='").append(endpoint).append('\'');
      sb.append(", projectName='").append(projectName).append('\'');
      sb.append(", tunnelEndpoint='").append(tunnelEndpoint).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  public static class TablesGroup {
    private List<Table> tables;
    //group config for tables.
    private Config groupTableConfig;

    public TablesGroup() {
    }

    public List<Table> getTables() {
      return tables;
    }

    public void setTables(List<Table> tables) {
      this.tables = tables;
    }

    public Config getGroupTableConfig() {
      return groupTableConfig;
    }

    public void setGroupTableConfig(Config groupTableConfig) {
      this.groupTableConfig = groupTableConfig;
    }
  }
  public static class Table {
    private String sourceDataBase;
    private String sourceTableName;
    private String destinationProject;
    private String destinationTableName;
    //table config.
    private Config tableConfig;

    public Table(String sourceDataBase,
                 String sourceTableName,
                 String destinationProject,
                 String destinationTableName,
                 Config tableConfig) {
      this.sourceDataBase = sourceDataBase;
      this.sourceTableName = sourceTableName;
      this.destinationProject = destinationProject;
      this.destinationTableName = destinationTableName;
      this.tableConfig = tableConfig;
    }
  }

  public static class Config {
    //key: original data type, value: current data type.
    private Map<String, String> typeCustomizedConversion;
    //key: original column name, value: current column name.
    private Map<String, String> columnNameCustomizedConversion;
    private int numOfPartitions;
    private int retryTimesLimit;
    private String whereCondition;

    public Config(Map<String, String> typeCustomizedConversion,
                  Map<String, String> columnNameCustomizedConversion,
                  int numOfPartitions,
                  int retryTimesLimit,
                  String whereCondition) {
      this.typeCustomizedConversion = typeCustomizedConversion;
      this.columnNameCustomizedConversion = columnNameCustomizedConversion;
      this.numOfPartitions = numOfPartitions;
      this.retryTimesLimit = retryTimesLimit;
      this.whereCondition = whereCondition;
    }

    public Map<String, String> getTypeCustomizedConversion() {
      return typeCustomizedConversion;
    }

    public Map<String, String> getColumnNameCustomizedConversion() {
      return columnNameCustomizedConversion;
    }

    public int getNumOfPartitions() {
      return numOfPartitions;
    }

    public int getRetryTimesLimit() {
      return retryTimesLimit;
    }

    public String getWhereCondition() {
      return whereCondition;
    }
  }

  public Config getTableConfig(String dataBase, String tableName) {
    if (this.tableConfig.containsKey(dataBase)) {
      if (this.tableConfig.get(dataBase).containsKey(tableName)) {
        return this.tableConfig.get(dataBase).get(tableName).tableConfig;
      }
    }
    return null;
  }

  public boolean validateAndInitConfig() {
    boolean validated = true;
    switch (this.dataSource) {
      case Hive:
        if (!this.hiveMetaSource.validate()) {
          validated = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.hiveMetaSource);
        }
        break;
      case OSS:
        if (!this.ossDataSource.validate()) {
          validated = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.ossDataSource);
        }
        break;
    }
    if (odpsConfiguration.validate()) {
      validated = false;
      LOG.error("Validate MetaConfiguration failed due to {}", this.odpsConfiguration);
    }

    if (!validated) {
      return false;
    }

    this.migrationJobId = createMigrationJobId();
    for (TablesGroup tablesGroup : tablesGroupList) {
      for (Table table : tablesGroup.tables) {
        this.tableConfig.putIfAbsent(table.sourceDataBase, new HashMap<>());
        if (table.tableConfig == null) {
          if (tablesGroup.groupTableConfig != null) {
            table.tableConfig = tablesGroup.groupTableConfig;
          } else if (this.globalTableConfig != null) {
            table.tableConfig = globalTableConfig;
          }
        }
        this.tableConfig.get(table.sourceDataBase).put(table.sourceTableName, table);
      }
    }
    return true;
  }

}
