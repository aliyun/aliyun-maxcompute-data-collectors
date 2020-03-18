package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.List;
import java.util.Map;

// TODO: Split this class into several classes
import com.aliyun.odps.utils.StringUtils;

public class MmaConfig {

  public interface Config {
    boolean validate();
  }

  public static class OssConfig implements Config {
    private String ossEndpoint;
    private String ossBucket;

    public OssConfig(String ossEndpoint, String ossBucket) {
      this.ossEndpoint = ossEndpoint;
      this.ossBucket = ossBucket;
    }

    @Override
    public boolean validate() {
      // TODO: try to connect
      return (!StringUtils.isNullOrEmpty(ossEndpoint) &&
          !StringUtils.isNullOrEmpty(ossBucket));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OssDataSource {");
      sb.append("ossEndpoint='").append(ossEndpoint).append('\'');
      sb.append(", ossBucket='").append(ossBucket).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  public static class HiveConfig implements Config {
    private String jdbcConnectionUrl;
    private String user;
    private String password;
    private String hmsThriftAddr;
    private String krbPrincipal;
    private String keyTab;
    private List<String> krbSystemProperties;
    private List<String> hiveJdbcExtraSettings;

    public HiveConfig(String jdbcConnectionUrl,
                             String user,
                             String password,
                             String hmsThriftAddr,
                             String krbPrincipal,
                             String keyTab,
                             List<String> krbSystemProperties,
                             List<String> hiveJdbcExtraSettings) {
      this.jdbcConnectionUrl = jdbcConnectionUrl;
      this.user = user;
      this.password = password;
      this.hmsThriftAddr = hmsThriftAddr;
      this.krbPrincipal = krbPrincipal;
      this.keyTab = keyTab;
      this.krbSystemProperties = krbSystemProperties;
      this.hiveJdbcExtraSettings = hiveJdbcExtraSettings;
    }

    @Override
    public boolean validate() {
      if (hiveJdbcExtraSettings == null || hiveJdbcExtraSettings.isEmpty()) {
        return false;
      }
      for (String setting : hiveJdbcExtraSettings) {
        if (StringUtils.isNullOrEmpty(setting)) {
          return false;
        }
      }
      return (!StringUtils.isNullOrEmpty(jdbcConnectionUrl) &&
              !StringUtils.isNullOrEmpty(hmsThriftAddr));
    }

    public String getJdbcConnectionUrl() {
      return jdbcConnectionUrl;
    }

    public String getUser() {
      return user;
    }

    public String getPassword() {
      return password;
    }

    public String getHmsThriftAddr() {
      return hmsThriftAddr;
    }

    public String getKrbPrincipal() {
      return krbPrincipal;
    }

    public String getKeyTab() {
      return keyTab;
    }

    public List<String> getKrbSystemProperties() {
      return krbSystemProperties;
    }

    public List<String> getHiveJdbcExtraSettings() {
      return hiveJdbcExtraSettings;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("HiveConfig {");
      sb.append("hiveJdbcAddress='").append(jdbcConnectionUrl).append('\'');
      sb.append(", hmsThriftAddr='").append(hmsThriftAddr).append('\'');
      sb.append(", krbPrincipal='").append(krbPrincipal).append('\'');
      sb.append(", keyTab='").append(keyTab).append('\'');
      sb.append(", krbSystemProperties=").append(String.join(", ", krbSystemProperties));
      sb.append(", hiveJdbcExtraSettings=").append(String.join(",", hiveJdbcExtraSettings));
      sb.append('}');
      return sb.toString();
    }
  }

  public static class OdpsConfig implements Config {
    private String accessId;
    private String accessKey;
    private String endpoint;
    private String projectName;
    private String tunnelEndpoint;

    public OdpsConfig(String accessId,
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

    @Override
    public boolean validate() {
      return (!StringUtils.isNullOrEmpty(accessId) &&
              !StringUtils.isNullOrEmpty(accessKey) &&
              !StringUtils.isNullOrEmpty(endpoint) &&
              !StringUtils.isNullOrEmpty(projectName));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("OdpsConfig {");
      sb.append("accessId='").append(accessId).append('\'');
      sb.append(", accessKey='").append(accessKey).append('\'');
      sb.append(", endpoint='").append(endpoint).append('\'');
      sb.append(", projectName='").append(projectName).append('\'');
      sb.append(", tunnelEndpoint='").append(tunnelEndpoint).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }

  public static class ServiceMigrationConfig implements Config {
    private String destProjectName;

    public ServiceMigrationConfig (String destProjectName) {
      this.destProjectName = destProjectName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    @Override
    public boolean validate() {
      return !StringUtils.isNullOrEmpty(destProjectName);
    }
  }

  public static class DatabaseMigrationConfig implements Config {
    private String sourceDatabaseName;
    private String destProjectName;
    private AdditionalTableConfig additionalTableConfig;

    public DatabaseMigrationConfig (String sourceDatabaseName,
                                    String destProjectName,
                                    AdditionalTableConfig additionalTableConfig) {
      this.sourceDatabaseName = sourceDatabaseName;
      this.destProjectName = destProjectName;
      this.additionalTableConfig = additionalTableConfig;
    }

    public String getSourceDatabaseName() {
      return sourceDatabaseName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    @Override
    public boolean validate() {
      return !StringUtils.isNullOrEmpty(sourceDatabaseName)
             && !StringUtils.isNullOrEmpty(destProjectName)
             && (additionalTableConfig == null || additionalTableConfig.validate());
    }
  }

  public static class TableMigrationConfig implements Config {
    private String sourceDataBaseName;
    private String sourceTableName;
    private String destProjectName;
    private String destTableName;
    private List<List<String>> partitionValuesList;
    private AdditionalTableConfig additionalTableConfig;

    public TableMigrationConfig (String sourceDataBaseName,
                                 String sourceTableName,
                                 String destProjectName,
                                 String destTableName,
                                 AdditionalTableConfig additionalTableConfig) {
      this(sourceDataBaseName,
           sourceTableName,
           destProjectName,
           destTableName,
           null,
           additionalTableConfig);
    }

    public TableMigrationConfig (String sourceDataBaseName,
                                 String sourceTableName,
                                 String destProjectName,
                                 String destTableName,
                                 List<List<String>> partitionValuesList,
                                 AdditionalTableConfig additionalTableConfig) {
      this.sourceDataBaseName = sourceDataBaseName;
      this.sourceTableName = sourceTableName;
      this.destProjectName = destProjectName;
      this.destTableName = destTableName;
      this.partitionValuesList = partitionValuesList;
      this.additionalTableConfig = additionalTableConfig;
    }

    public String getSourceDataBaseName() {
      return sourceDataBaseName;
    }

    public String getSourceTableName() {
      return sourceTableName;
    }

    public String getDestProjectName() {
      return destProjectName;
    }

    public String getDestTableName() {
      return destTableName;
    }

    public List<List<String>> getPartitionValuesList() {
      return partitionValuesList;
    }

    public AdditionalTableConfig getAdditionalTableConfig() {
      return additionalTableConfig;
    }

    public void setAdditionalTableConfig(AdditionalTableConfig additionalTableConfig) {
      this.additionalTableConfig = additionalTableConfig;
    }

    public void apply(MetaSource.TableMetaModel tableMetaModel) {
      // TODO: use typeCustomizedConversion and columnNameCustomizedConversion
      tableMetaModel.odpsProjectName = destProjectName;
      tableMetaModel.odpsTableName = destTableName;
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

      // TODO: make it a general config
      for (MetaSource.ColumnMetaModel pc : tableMetaModel.partitionColumns) {
        if ("DATE".equalsIgnoreCase(pc.type)) {
          pc.odpsType = "STRING";
        }
      }
    }

    public static TableMigrationConfig fromJson(String json) {
      return GsonUtils.getFullConfigGson().fromJson(json, TableMigrationConfig.class);
    }

    public static String toJson(TableMigrationConfig config) {
      return GsonUtils.getFullConfigGson().toJson(config);
    }

    @Override
    public boolean validate() {
      return !StringUtils.isNullOrEmpty(sourceDataBaseName)
             && !StringUtils.isNullOrEmpty(sourceTableName)
             && !StringUtils.isNullOrEmpty(destProjectName)
             && !StringUtils.isNullOrEmpty(destTableName)
             && partitionValuesList == null || partitionValuesList.stream().noneMatch(List::isEmpty)
             && (additionalTableConfig == null || additionalTableConfig.validate());
    }
  }

  public static class AdditionalTableConfig implements Config {
    //key: original data type, value: current data type.
    private Map<String, String> typeCustomizedConversion;
    //key: original column name, value: current column name.
    private Map<String, String> columnNameCustomizedConversion;
    private int partitionGroupSize;
    private int retryTimesLimit;

    public AdditionalTableConfig(Map<String, String> typeCustomizedConversion,
                                 Map<String, String> columnNameCustomizedConversion,
                                 int partitionGroupSize,
                                 int retryTimesLimit) {
      this.typeCustomizedConversion = typeCustomizedConversion;
      this.columnNameCustomizedConversion = columnNameCustomizedConversion;
      this.partitionGroupSize = partitionGroupSize;
      this.retryTimesLimit = retryTimesLimit;
    }

    public Map<String, String> getTypeCustomizedConversion() {
      return typeCustomizedConversion;
    }

    public Map<String, String> getColumnNameCustomizedConversion() {
      return columnNameCustomizedConversion;
    }

    public int getPartitionGroupSize() {
      return partitionGroupSize;
    }

    public int getRetryTimesLimit() {
      return retryTimesLimit;
    }

    @Override
    public boolean validate() {
      // TODO: validate arguments
      return true;
    }
  }
}
