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
package com.netflix.maestro.models.trigger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.annotations.SuppressFBWarnings;
import com.netflix.maestro.models.parameter.SignalParamDefinition;
import java.util.Map;
import lombok.Data;

/** Request payload for signal triggers. */
@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Data
public class SignalTrigger {
  private String[] joinKeys;
  private Map<String, Map<String, SignalParamDefinition>> definition;
  @Nullable private String signalTriggerId;

  /** Setter. */
  public void setDefinition(Map<String, Map<String, SignalParamDefinition>> definition) {
    definition
        .values()
        .forEach(signalParams -> signalParams.forEach((k, v) -> v.getParameter().setName(k)));
    this.definition = definition;
  }
}
