package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.action.info.VerificationActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

public class VerificationAction extends AbstractAction {

  private static final Logger LOG = LogManager.getLogger(VerificationAction.class);

  public VerificationAction(String id) {
    super(id);
    actionInfo = new VerificationActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    List<List<String>> sourceVerificationResult =
        actionExecutionContext.getSourceVerificationResult();
    List<List<String>> destVerificationResult =
        actionExecutionContext.getDestVerificationResult();

    boolean passed = true;

    if (sourceVerificationResult == null || destVerificationResult == null) {
      LOG.warn("source/dest verification results not found, actionId: {}", id);
    } else {
      int partitionColumnCount = actionExecutionContext.getTableMetaModel().partitionColumns.size();

      if (partitionColumnCount == 0) {
        assert sourceVerificationResult.size() == 1;
        assert sourceVerificationResult.get(0).size() == 1;
        assert destVerificationResult.size() == 1;
        assert sourceVerificationResult.get(0).size() == 1;

        Long sourceRecordCount = Long.valueOf(sourceVerificationResult.get(0).get(0));
        Long destRecordCount = Long.valueOf(destVerificationResult.get(0).get(0));
        passed = sourceRecordCount.equals(destRecordCount);
      } else {
        List<List<String>> succeededPartitions = new LinkedList<>();
        List<List<String>> failedPartitions = new LinkedList<>();

        for (PartitionMetaModel partitionMetaModel :
            actionExecutionContext.getTableMetaModel().partitions) {

          List<String> partitionValues = partitionMetaModel.partitionValues;
          List<String> sourceRecordCount = sourceVerificationResult
              .stream()
              .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
              .map(r -> r.get(partitionColumnCount))
              .collect(Collectors.toList());
          List<String> destRecordCount = destVerificationResult
              .stream()
              .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
              .map(r -> r.get(partitionColumnCount))
              .collect(Collectors.toList());

          // When partition is empty, foundInSource and foundInDest are both false.
          if (sourceRecordCount.isEmpty() && destRecordCount.isEmpty()) {
            LOG.warn("Ignored Empty partition: {}, actionId: {}", partitionValues, id);
            succeededPartitions.add(partitionValues);
          } else if (sourceRecordCount.isEmpty()) {
            LOG.warn("Ignored unexpected partition: {}, actionId: {}",
                     partitionValues, id);
            succeededPartitions.add(partitionValues);
          } else if (destRecordCount.isEmpty()) {
            LOG.error("Dest lacks partition: {}, actionId: {}", partitionValues, id);
            failedPartitions.add(partitionValues);
            passed = false;
          } else {
            Long source = Long.valueOf(sourceRecordCount.get(0));
            Long dest = Long.valueOf(destRecordCount.get(0));
            if (!dest.equals(source)) {
              LOG.error("Record number not matched, source: {}, dest: {}, actionId: {}",
                        source, dest, id);
              passed = false;
              failedPartitions.add(partitionValues);
            } else {
              succeededPartitions.add(partitionValues);
            }
          }
        }

        ((VerificationActionInfo) actionInfo).setSucceededPartitions(succeededPartitions);
        ((VerificationActionInfo) actionInfo).setFailedPartitions(failedPartitions);
      }
    }

    if (passed) {
      setProgress(ActionProgress.SUCCEEDED);
    } else {
      setProgress(ActionProgress.FAILED);
    }
  }

  @Override
  public void afterExecution() {
  }

  @Override
  public boolean executionFinished() {
    return true;
  }

  @Override
  public void stop() {
  }
}
