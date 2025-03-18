package com.netflix.maestro.engine.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.signal.messageprocessors.SignalTriggerMatchProcessor;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;

/**
 * Listener class to configure SignalTriggerMatch queue. It should be based on a FIFO queue
 * partitioned by (workflow_id, trigger_uuid) to reduce race conditions.
 *
 * @author jun-he
 */
@AllArgsConstructor
@Slf4j
public class SqsSignalTriggerMatchListener {
  private final SignalTriggerMatchProcessor processor;
  private final ObjectMapper objectMapper;

  /** Listener configuration for SQS SignalTriggerMatch message. */
  @SqsListener(
      value = "${aws.sqs.signal-trigger-match-queue-url}",
      deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
  public void process(String payload) {
    LOG.info("SqsSignalTriggerMatchListener got message: [{}]", payload);
    processor.process(
        () -> {
          try {
            return objectMapper.readValue(payload, SignalTriggerMatch.class);
          } catch (IOException ex) {
            throw new MaestroInternalError(ex, "exception during json parsing");
          }
        });
  }
}
