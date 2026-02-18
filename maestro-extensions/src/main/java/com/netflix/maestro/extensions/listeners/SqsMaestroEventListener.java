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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.extensions.processors.MaestroEventProcessor;
import com.netflix.maestro.models.events.MaestroEvent;
import io.awspring.cloud.sqs.annotation.SqsListener;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Listener class for consuming MaestroEvent messages from SQS. */
@AllArgsConstructor
@Slf4j
@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SqsMaestroEventListener {
  private static final String SNS_MESSAGE_FIELD = "Message";

  private final MaestroEventProcessor processor;
  private final ObjectMapper objectMapper;

  /**
   * Listener for SQS MaestroEvent messages. Handles both raw messages and SNS-wrapped messages
   * (where the actual payload is in the "Message" field of the SNS envelope).
   */
  @SqsListener(value = "${extensions.maestro-event-queue-url}", acknowledgementMode = "ON_SUCCESS")
  public void process(String payload) {
    LOG.info("SqsMaestroEventListener got message: [{}]", payload);
    try {
      String eventPayload = unwrapSnsEnvelope(payload);
      MaestroEvent event = objectMapper.readValue(eventPayload, MaestroEvent.class);
      processor.process(event);
    } catch (IOException ex) {
      throw new MaestroInternalError(ex, "exception during json parsing");
    }
  }

  /**
   * If the payload is an SNS notification envelope (contains "Type" and "Message" fields), extract
   * the inner message payload. Otherwise return the payload as-is.
   */
  private String unwrapSnsEnvelope(String payload) throws IOException {
    JsonNode root = objectMapper.readTree(payload);
    if (root.has("Type") && root.has(SNS_MESSAGE_FIELD)) {
      return root.get(SNS_MESSAGE_FIELD).asText();
    }
    return payload;
  }
}
