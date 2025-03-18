package com.netflix.maestro.signal.messageprocessors;

import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * It processes {@link SignalTriggerMatch} message. This message processor is responsible to verify
 * if this match can lead to a workflow execution. If yes, it will send out an execution message to
 * run the workflow instance and update the checkpoint. It also does the cleanup based on the query
 * result. The most of the logics happens within a transaction locked by a ro in
 * maestro_signal_trigger table.
 *
 * @author jun-he
 */
@Slf4j
@AllArgsConstructor
public class SignalTriggerMatchProcessor {
  private final MaestroSignalBrokerDao brokerDao;
  private final MaestroMetrics metrics;

  /**
   * Process the signal trigger match based on the received signal instance, which might lead to an
   * execution. It should be based on a fifo queue partitioned by (workflow, trigger_uuid) to reduce
   * race conditions.
   *
   * @param messageSupplier message supplier
   */
  public void process(Supplier<SignalTriggerMatch> messageSupplier) {
    SignalTriggerMatch triggerMatch = messageSupplier.get();
    try {
      int status = brokerDao.tryExecuteTrigger(triggerMatch);
      if (status == -1) {
        LOG.info("Delete invalid workflow signal trigger: [{}]", triggerMatch);
        brokerDao.deleteTrigger(triggerMatch.getWorkflowId(), triggerMatch.getTriggerUuid());
      }
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_MATCH_SUCCESS,
          getClass(),
          MetricConstants.TYPE_TAG,
          String.valueOf(status));
    } catch (RuntimeException e) {
      if (e instanceof MaestroRetryableError) {
        metrics.counter(
            MetricConstants.SIGNAL_TRIGGER_MATCH_FAILURE,
            getClass(),
            MetricConstants.TYPE_TAG,
            "retryable");
        throw e;
      }
      metrics.counter(
          MetricConstants.SIGNAL_TRIGGER_MATCH_FAILURE,
          getClass(),
          MetricConstants.TYPE_TAG,
          "recoverable");
      throw new MaestroRetryableError(e, "will retry the error for [%s]", triggerMatch);
    }
  }
}
