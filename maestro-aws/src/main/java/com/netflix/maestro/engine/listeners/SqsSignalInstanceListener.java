package com.netflix.maestro.engine.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.messageprocessors.SignalInstanceProcessor;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;

/**
 * Listener class to configure SignalInstance queue. It should be based on a FIFO queue partitioned
 * by signal name to reduce race conditions.
 *
 * @author jun-he
 */
@AllArgsConstructor
@Slf4j
public class SqsSignalInstanceListener {
  private final SignalInstanceProcessor processor;
  private final ObjectMapper objectMapper;

  /** Listener configuration for SQS SignalInstance message. */
  @SqsListener(
      value = "${aws.sqs.signal-instance-queue-url}",
      deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
  public void process(String payload) {
    LOG.info("SqsSignalInstanceListener got message: [{}]", payload);
    processor.process(
        () -> {
          try {
            return objectMapper.readValue(payload, SignalInstance.class);
          } catch (IOException ex) {
            throw new MaestroInternalError(ex, "exception during json parsing");
          }
        });
  }
}
