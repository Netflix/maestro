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
import com.netflix.maestro.models.parameter.Parameter;
import com.netflix.maestro.models.trigger.TriggerUuids;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

/** Signal Initiator to start a workflow instance. */
@EqualsAndHashCode(callSuper = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"trigger_uuid", "depth", "type", "signal_instance_ids", "params"},
    alphabetic = true)
@Data
public class SignalInitiator extends TriggerInitiator {
  private List<String> signalInstanceIds;
  private Map<String, Parameter> params;

  @Override
  public boolean isValid(TriggerUuids triggerUuids) {
    if (triggerUuids == null || triggerUuids.getSignalTriggerUuids() == null) {
      return false;
    }
    return triggerUuids.getSignalTriggerUuids().containsKey(getTriggerUuid());
  }

  @Override
  public Type getType() {
    return Type.SIGNAL;
  }
}
