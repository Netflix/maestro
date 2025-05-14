package com.netflix.maestro.engine.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerExecutionProcessor;
import com.netflix.maestro.signal.models.SignalTriggerExecution;
import io.awspring.cloud.sqs.annotation.SqsListener;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Listener class to configure SignalTriggerExecution queue. It should be based on a FIFO queue
 * partitioned by (workflow_id, trigger_uuid) to reduce race conditions.
 *
 * @author jun-he
 */
@AllArgsConstructor
@Slf4j
public class SqsSignalTriggerExecutionListener {
  private final SignalTriggerExecutionProcessor processor;
  private final ObjectMapper objectMapper;

  /** Listener configuration for SQS SignalTriggerExecution message. */
  @SqsListener(
      value = "${aws.sqs.signal-trigger-execution-queue-url}",
      acknowledgementMode = "ON_SUCCESS")
  public void process(String payload) {
    LOG.info("SqsSignalTriggerExecutionListener got message: [{}]", payload);
    processor.process(
        () -> {
          try {
            return objectMapper.readValue(payload, SignalTriggerExecution.class);
          } catch (IOException ex) {
            throw new MaestroInternalError(ex, "exception during json parsing");
          }
        });
  }
}
