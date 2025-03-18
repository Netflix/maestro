package com.netflix.maestro.signal.producer;

import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import com.netflix.maestro.signal.models.SignalTriggerMatch;

/**
 * Queue producer for signal related messages.
 *
 * @author jun-he
 */
public interface SignalQueueProducer {
  /**
   * Push a signal instance.
   *
   * @param signalInstance the signal instance to push
   */
  void push(SignalInstance signalInstance);

  /**
   * Push a signal trigger match.
   *
   * @param triggerMatch the signal trigger match to push
   */
  void push(SignalTriggerMatch triggerMatch);

  /**
   * Push a signal trigger execution.
   *
   * @param triggerExecution the signal trigger execution to push
   */
  void push(SignalTriggerExecution triggerExecution);
}
