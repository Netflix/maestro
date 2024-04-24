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
package com.netflix.maestro.models.initiator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.timeline.TimelineEvent;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import com.netflix.maestro.models.trigger.TriggerUuids;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Trigger initiator for time or signal based triggers. trigger_uuid is used to check if the signal
 * trigger is valid.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(alphabetic = true)
@Data
public abstract class TriggerInitiator implements Initiator {
  @NotNull private String triggerUuid;

  private int depth;

  /**
   * check if the initiator is valid based on the trigger uuids in the current active workflow
   * version.
   */
  @JsonIgnore
  public abstract boolean isValid(TriggerUuids triggerUuids);

  @JsonIgnore
  @Override
  public TimelineEvent getTimelineEvent() {
    return TimelineLogEvent.info(
        "%s-based trigger runs a new workflow instance by trigger uuid [%s]",
        getType().name(), triggerUuid);
  }
}
