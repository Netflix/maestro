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
import com.netflix.maestro.queue.jobevents.MaestroJobEvent;
import com.netflix.maestro.signal.models.SignalTriggerMatch;
import lombok.Data;

/**
 * Maestro internal job event to process a signal trigger match. The signal trigger match will be
 * enqueued via MaestroQueueSystem to ensure exactly-once semantics and transactional safety.
 *
 * @author maestro
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
@Data
public class SignalTriggerMatchJobEvent implements MaestroJobEvent {
  private SignalTriggerMatch triggerMatch;

  /** Static creator. */
  public static SignalTriggerMatchJobEvent create(SignalTriggerMatch triggerMatch) {
    SignalTriggerMatchJobEvent jobEvent = new SignalTriggerMatchJobEvent();
    jobEvent.setTriggerMatch(triggerMatch);
    return jobEvent;
  }

  @Override
  public Type getType() {
    return Type.SIGNAL;
  }
}
