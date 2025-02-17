/*
 * Copyright 2025 Netflix, Inc.
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
package com.netflix.maestro.engine.publisher;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroRetryableError;
import com.netflix.maestro.exceptions.MaestroUnprocessableEntityException;
import com.netflix.maestro.models.events.MaestroEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** SNS Event notification publisher implementation for MaestroNotificationPublisher. */
@Slf4j
@AllArgsConstructor
public class SnsEventNotificationPublisher implements MaestroNotificationPublisher {
  private final AmazonSNS amazonSns;
  private final String snsTopic;
  private final ObjectMapper objectMapper;

  /** Send a Maestro event to the SNS topic. */
  public void send(MaestroEvent event) {
    try {
      String payload = objectMapper.writeValueAsString(event);
      PublishResult result = amazonSns.publish(snsTopic, payload);
      LOG.info(
          "Published a maestro event [{}] to sns with message id: [{}]",
          payload,
          result.getMessageId());
    } catch (JsonProcessingException je) {
      throw new MaestroUnprocessableEntityException("cannot serialize maestro event: " + event, je);
    } catch (RuntimeException e) {
      throw new MaestroRetryableError(
          e, "Failed to publish a maestro event to sns and will retry the event publishing.");
    }
  }
}
