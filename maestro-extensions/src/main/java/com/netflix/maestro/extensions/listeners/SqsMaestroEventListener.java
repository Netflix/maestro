/*
 * Copyright 2024 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.maestro.extensions.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.models.events.MaestroEvent;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.annotation.SqsListenerAcknowledgementMode;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.Header;

/**
 * Listener class for consuming MaestroEvent messages from SQS. Matches internal's {@code
 * SqsMaestroEventListener} pattern with manual acknowledgment, visibility extension on failure, and
 * receive count tracking. Inlines the logic from internal's {@code SqsProcessorFinalizer}.
 */
@AllArgsConstructor
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SqsMaestroEventListener {
  private static final int RECEIVE_COUNT_THRESHOLD_FOR_ADDITIONAL_MONITORING = 100;
  private static final int VISIBILITY_TIMEOUT_ON_FAILURE_SECS = 0;

  private final MaestroEventProcessor processor;
  private final ObjectMapper objectMapper;

  /**
   * Listener for SQS MaestroEvent messages. Uses manual acknowledgment matching internal's {@code
   * SqsProcessorFinalizer} pattern: acknowledge on success, extend visibility on retryable failure,
   * acknowledge (delete) on non-retryable failure (e.g., deserialization errors).
   */
  @SqsListener(
      value = "${extensions.maestro-event-queue-url}",
      acknowledgementMode = SqsListenerAcknowledgementMode.MANUAL)
  public void process(
      String payload,
      Acknowledgement acknowledgement,
      Visibility visibility,
      @Header(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT) int receiveCount) {
    try {
      MaestroEvent event = objectMapper.readValue(payload, MaestroEvent.class);
      processor.process(event);
      acknowledgement.acknowledge();
    } catch (IOException ex) {
      // Non-retryable: deserialization will always fail for this message, so delete it
      LOG.warn(
          "Deleting non-retryable message from queue. Exception {} when processing payload {}",
          ex.getClass().getSimpleName(),
          payload,
          ex);
      acknowledgement.acknowledge();
    } catch (Exception ex) {
      if (receiveCount >= RECEIVE_COUNT_THRESHOLD_FOR_ADDITIONAL_MONITORING) {
        LOG.warn(
            "SQS payload has been retrying {} times, check if there are any problems: {}",
            receiveCount,
            payload);
      }
      LOG.warn(
          "Exception {} when processing MaestroEvent, to be retried later",
          ex.getClass().getSimpleName(),
          ex);
      visibility.changeTo(VISIBILITY_TIMEOUT_ON_FAILURE_SECS);
    }
  }
}
