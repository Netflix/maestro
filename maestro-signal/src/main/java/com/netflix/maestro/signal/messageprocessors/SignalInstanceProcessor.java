package com.netflix.maestro.signal.messageprocessors;

import com.netflix.maestro.metrics.MaestroMetrics;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.models.signal.SignalTriggerMatch;
import com.netflix.maestro.signal.dao.MaestroSignalBrokerDao;
import com.netflix.maestro.signal.metrics.MetricConstants;
import com.netflix.maestro.signal.producer.SignalQueueProducer;
import com.netflix.maestro.utils.Checks;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * It processes {@link SignalInstance} message. This message processor is responsible to find
 * matching subscriptions which can be satisfied by the incoming {@link SignalInstance} message.
 *
 * @author jun-he
 */
@Slf4j
@AllArgsConstructor
public class SignalInstanceProcessor {
  private final MaestroSignalBrokerDao brokerDao;
  private final SignalQueueProducer producer;
  private final MaestroMetrics metrics;

  /**
   * Process the signal instance. It should be based on a FIFO queue partitioned by signal name to
   * reduce race conditions.
   *
   * @param messageSupplier the message supplier
   */
  public void process(Supplier<SignalInstance> messageSupplier) {
    SignalInstance signalInstance = messageSupplier.get();
    Checks.checkTrue(
        signalInstance.getSeqId() > 0,
        "Invalid seq id for [%s], it must be positive",
        signalInstance);

    List<SignalTriggerMatch> triggerMatches = brokerDao.getSubscribedTriggers(signalInstance);

    if (triggerMatches.isEmpty()) {
      LOG.debug("No matching signal subscription found for signal instance [{}]", signalInstance);
      return;
    }
    for (SignalTriggerMatch triggerMatch : triggerMatches) {
      producer.push(triggerMatch);
      metrics.counter(MetricConstants.SIGNAL_TRIGGER_MATCH_FOUND, getClass());
    }
  }
}
