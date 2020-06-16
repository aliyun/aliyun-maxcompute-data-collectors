package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;
import com.aliyun.odps.task.SQLTask;

public class OdpsSqlExecutor extends AbstractActionExecutor {

  private static final Logger LOG = LogManager.getLogger(OdpsSqlExecutor.class);

  private static class OdpsSqlCallable implements Callable<List<List<String>>> {

    private String endpoint;
    private String accessId;
    private String accessKey;
    private String project;
    private String sql;
    private Map<String, String> settings;
    private String actionId;
    private OdpsSqlActionInfo odpsSqlActionInfo;

    OdpsSqlCallable(
        String endpoint,
        String accessId,
        String accessKey,
        String project,
        String sql,
        Map<String, String> settings,
        String actionId,
        OdpsSqlActionInfo odpsSqlActionInfo) {
      this.endpoint = Objects.requireNonNull(endpoint);
      this.accessId = Objects.requireNonNull(accessId);
      this.accessKey = Objects.requireNonNull(accessKey);
      this.project = Objects.requireNonNull(project);
      this.sql = Objects.requireNonNull(sql);
      this.settings = Objects.requireNonNull(settings);
      this.actionId = Objects.requireNonNull(actionId);
      this.odpsSqlActionInfo = Objects.requireNonNull(odpsSqlActionInfo);
    }

    @Override
    public List<List<String>> call() throws Exception {
      LOG.info("Executing sql: {}", sql);

      Account account = new AliyunAccount(accessId, accessKey);
      Odps odps = new Odps(account);
      odps.setEndpoint(endpoint);
      odps.setDefaultProject(project);

      Instance i = SQLTask.run(odps, odps.getDefaultProject(), sql, settings, null);

      odpsSqlActionInfo.setInstanceId(i.getId());
      LOG.info("InstanceId: {}, actionId: {}", i.getId(), actionId);

      try {
        odpsSqlActionInfo.setLogView(odps.logview().generateLogView(i, 72));
      } catch (OdpsException ignore) {
      }

      i.waitForSuccess();

      return parseResult(i);
    }

    private List<List<String>> parseResult(Instance instance) throws OdpsException {
      List<Record> records = SQLTask.getResult(instance);
      List<List<String>> ret = new LinkedList<>();

      int columnCount;
      if (records.isEmpty()) {
        return ret;
      } else {
        columnCount = records.get(0).getColumnCount();
      }

      for (Record r : records) {
        List<String> row = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
          row.add(r.get(i).toString());
        }
        ret.add(row);
      }

      return ret;
    }
  }

  public Future<List<List<String>>> execute(
      String sql,
      Map<String, String> settings,
      String actionId,
      OdpsSqlActionInfo odpsSqlActionInfo) {
    // TODO: endpoint, ak, project name should come with tableMigrationConfig

    OdpsSqlCallable callable = new OdpsSqlCallable(
        MmaServerConfig.getInstance().getOdpsConfig().getEndpoint(),
        MmaServerConfig.getInstance().getOdpsConfig().getAccessId(),
        MmaServerConfig.getInstance().getOdpsConfig().getAccessKey(),
        MmaServerConfig.getInstance().getOdpsConfig().getProjectName(),
        sql,
        settings,
        actionId,
        odpsSqlActionInfo);

    return executor.submit(callable);
  }
}
