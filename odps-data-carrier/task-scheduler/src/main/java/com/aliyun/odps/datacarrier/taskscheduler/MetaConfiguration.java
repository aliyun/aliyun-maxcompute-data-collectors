package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.utils.StringUtils;

// TODO: rename
public class MetaConfiguration {
  private static final Logger LOG = LogManager.getLogger(MetaConfiguration.class);

  private String user;
  private String migrationJobName;
  private volatile String migrationJobId;

  //basic environment configuration
  private DataSource dataSource;
  private OssConfiguration ossConfiguration;
  private HiveConfiguration hiveConfiguration;
  private OdpsConfiguration odpsConfiguration;
  private List<TableGroup> tableGroups;

  //global config for all tables.
  private Config globalTableConfig;

  private volatile Map<String, Map<String, TableConfig>> tableConfigMap;

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

  public OssConfiguration getOssConfiguration() {
    return ossConfiguration;
  }

  public void setOssConfiguration(OssConfiguration ossConfiguration) {
    this.ossConfiguration = ossConfiguration;
  }

  public HiveConfiguration getHiveConfiguration() {
    return hiveConfiguration;
  }

  public void setHiveConfiguration(HiveConfiguration hiveConfiguration) {
    this.hiveConfiguration = hiveConfiguration;
  }

  public OdpsConfiguration getOdpsConfiguration() {
    return odpsConfiguration;
  }

  public void setOdpsConfiguration(OdpsConfiguration odpsConfiguration) {
    this.odpsConfiguration = odpsConfiguration;
  }

  public List<TableGroup> getTableGroups() {
    return tableGroups;
  }

  public void setTableGroups(List<TableGroup> tableGroups) {
    this.tableGroups = tableGroups;
  }

  public Config getGlobalTableConfig() {
    return globalTableConfig;
  }

  public void setGlobalTableConfig(Config globalTableConfig) {
    this.globalTableConfig = globalTableConfig;
  }

  public static class OssConfiguration {
    private String ossEndpoint;
    private String ossBucket;

    public OssConfiguration(String ossEndpoint, String ossBucket) {
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

  public static class HiveConfiguration {
    private String hiveJdbcAddress;
    private String user;
    private String password;
    private String thriftAddr;
    private String krbPrincipal;
    private String keyTab;
    private String[] krbSystemProperties;

    public HiveConfiguration(String hiveJdbcAddress,
                             String user,
                             String password,
                             String thriftAddr,
                             String krbPrincipal,
                             String keyTab,
                             String[] krbSystemProperties) {
      this.hiveJdbcAddress = hiveJdbcAddress;
      this.user = user;
      this.password = password;
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

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
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
      final StringBuilder sb = new StringBuilder("HiveConfiguration {");
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

    public String getAccessId() {
      return accessId;
    }

    public String getAccessKey() {
      return accessKey;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public String getProjectName() {
      return projectName;
    }

    public String getTunnelEndpoint() {
      return tunnelEndpoint;
    }

    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(accessId) &&
              !StringUtils.isNullOrEmpty(accessKey) &&
              !StringUtils.isNullOrEmpty(endpoint) &&
              !StringUtils.isNullOrEmpty(projectName));
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

  public static class TableGroup {
    private List<TableConfig> tableConfigs;
    //group config for tables.
    private Config config;

    public TableGroup() {
    }

    public List<TableConfig> getTableConfigs() {
      return tableConfigs;
    }

    public void setTables(List<TableConfig> tableConfigs) {
      this.tableConfigs = tableConfigs;
    }

    public Config getGroupConfig() {
      return config;
    }

    public void setGroupConfig(Config groupConfig) {
      this.config = groupConfig;
    }
  }

  public static class TableConfig  {
    public String sourceDataBase;
    public String sourceTableName;
    public String destinationProject;
    public String destinationTableName;
    public List<List<String>> partitionValuesList;
    //table config
    public Config config;

    public TableConfig (String sourceDataBase,
                        String sourceTableName,
                        String destinationProject,
                        String destinationTableName,
                        Config config) {
      this(sourceDataBase,
           sourceTableName,
           destinationProject,
           destinationTableName,
           null,
           config);
    }

    public TableConfig (String sourceDataBase,
                        String sourceTableName,
                        String destinationProject,
                        String destinationTableName,
                        List<List<String>> partitionValuesList,
                        Config config) {
      this.sourceDataBase = sourceDataBase;
      this.sourceTableName = sourceTableName;
      this.destinationProject = destinationProject;
      this.destinationTableName = destinationTableName;
      this.partitionValuesList = partitionValuesList;
      this.config = config;
    }

    public void apply(MetaSource.TableMetaModel tableMetaModel) {
      // TODO: use typeCustomizedConversion and columnNameCustomizedConversion
      tableMetaModel.odpsProjectName = destinationProject;
      tableMetaModel.odpsTableName = destinationTableName;
      // TODO: should not init a hive type transformer here, looking for better design
      TypeTransformer typeTransformer = new HiveTypeTransformer();

      for (MetaSource.ColumnMetaModel c : tableMetaModel.columns) {
        c.odpsColumnName = c.columnName;
        c.odpsType = typeTransformer.toOdpsTypeV2(c.type).getTransformedType();
      }

      for (MetaSource.ColumnMetaModel pc : tableMetaModel.partitionColumns) {
        pc.odpsColumnName = pc.columnName;
        pc.odpsType = typeTransformer.toOdpsTypeV2(pc.type).getTransformedType();
      }
    }

    public static TableConfig fromJson(String json) {
      return GsonUtils.getFullConfigGson().fromJson(json, TableConfig.class);
    }

    public static String toJson(TableConfig config) {
      return GsonUtils.getFullConfigGson().toJson(config);
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

    public Config() {
    }

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
    if (this.tableConfigMap.containsKey(dataBase)) {
      if (this.tableConfigMap.get(dataBase).containsKey(tableName)) {
        return this.tableConfigMap.get(dataBase).get(tableName).config;
      }
    }
    return null;
  }

  public boolean validateAndInitConfig() {
    boolean valid = true;
    switch (this.dataSource) {
      case Hive:
        if (!this.hiveConfiguration.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.hiveConfiguration);
        }
        break;
      case OSS:
        if (!this.ossConfiguration.validate()) {
          valid = false;
          LOG.error("Validate MetaConfiguration failed due to {}", this.ossConfiguration);
        }
        break;
    }

    if (!odpsConfiguration.validate()) {
      valid = false;
      LOG.error("Validate MetaConfiguration failed due to {}", this.odpsConfiguration);
    }

    if (!valid) {
      return false;
    }

    this.migrationJobId = createMigrationJobId();
    this.tableConfigMap = new HashMap<>();
    for (TableGroup tableGroup : tableGroups) {
      for (TableConfig table : tableGroup.tableConfigs) {
        this.tableConfigMap.putIfAbsent(table.sourceDataBase, new HashMap<>());
        if (table.config == null) {
          if (tableGroup.config != null) {
            table.config = tableGroup.config;
          } else if (this.globalTableConfig != null) {
            table.config = globalTableConfig;
          }
        }
        this.tableConfigMap.get(table.sourceDataBase).put(table.sourceTableName, table);
      }
    }
    return true;
  }

}
