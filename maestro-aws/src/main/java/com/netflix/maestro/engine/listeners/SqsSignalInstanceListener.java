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
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.signal.messageprocessors.SignalInstanceProcessor;
import io.awspring.cloud.sqs.annotation.SqsListener;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
  @SqsListener(value = "${aws.sqs.signal-instance-queue-url}", acknowledgementMode = "ON_SUCCESS")
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
