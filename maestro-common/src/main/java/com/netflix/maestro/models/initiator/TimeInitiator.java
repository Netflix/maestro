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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.trigger.TriggerUuids;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;

/** Cron Initiator of a workflow instance. */
@EqualsAndHashCode(callSuper = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"trigger_uuid", "trigger_time", "timezone", "depth", "type"},
    alphabetic = true)
@Data
public class TimeInitiator extends TriggerInitiator {
  @NotNull private Long triggerTime;
  @NotNull private String timezone;

  @Override
  public boolean isValid(TriggerUuids triggerUuids) {
    if (triggerUuids == null || triggerUuids.getTimeTriggerUuid() == null) {
      return false;
    }
    return triggerUuids.getTimeTriggerUuid().equals(getTriggerUuid());
  }

  @Override
  public Type getType() {
    return Type.TIME;
  }
}
