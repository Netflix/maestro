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
package com.netflix.maestro.engine.listeners;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.maestro.exceptions.MaestroInternalError;
import com.netflix.maestro.timetrigger.messageprocessors.TimeTriggerExecutionProcessor;
import com.netflix.maestro.timetrigger.models.TimeTriggerExecution;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.aws.messaging.listener.SqsMessageDeletionPolicy;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;

/**
 * Listener class to configure TimeTriggerExecution queue. It requires a delay queue support and has
 * a different settings from MaestroJobEvent listeners.
 */
@AllArgsConstructor
@Slf4j
public class SqsTimeTriggerExecutionListener {
  private final TimeTriggerExecutionProcessor processor;
  private final ObjectMapper objectMapper;

  /** Listener configuration for SQS TimeTriggerExecution message. */
  @SqsListener(
      value = "${aws.sqs.time-trigger-execution-queue-url}",
      deletionPolicy = SqsMessageDeletionPolicy.ON_SUCCESS)
  public void process(String payload) {
    LOG.info("TimeTriggerExecutionSqsListener got message: [{}]", payload);
    processor.process(
        () -> {
          try {
            return objectMapper.readValue(payload, TimeTriggerExecution.class);
          } catch (IOException ex) {
            throw new MaestroInternalError(ex, "exception during json parsing");
          }
        });
  }
}
