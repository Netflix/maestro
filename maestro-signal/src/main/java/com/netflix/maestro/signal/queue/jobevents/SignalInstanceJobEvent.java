/*
 * Copyright 2026 Netflix, Inc.
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
package com.netflix.maestro.signal.queue.jobevents;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.signal.SignalInstance;
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import lombok.Data;

/**
 * Maestro internal job event to process a signal instance. The signal instance will be enqueued via
 * MaestroQueueSystem to ensure exactly-once semantics and transactional safety.
 *
 * @author maestro
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
@Data
public class SignalInstanceJobEvent implements MaestroJobEvent {
  private SignalInstance signalInstance;

  /** Static creator. */
  public static SignalInstanceJobEvent create(SignalInstance signalInstance) {
    SignalInstanceJobEvent jobEvent = new SignalInstanceJobEvent();
    jobEvent.setSignalInstance(signalInstance);
    return jobEvent;
  }

  @Override
  public Type getType() {
    return Type.SIGNAL;
  }
}
