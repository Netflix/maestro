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
package com.netflix.maestro.models.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.parameter.MapParamDefinition;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.ToString;

/** Request payload for step signal outputs. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"type", "definitions"},
    alphabetic = true)
@Data
@ToString()
public class SignalOutputsDefinition implements StepOutputsDefinition {
  private static final String NAME_PARAM = "name";
  @NotNull private final List<MapParamDefinition> definitions;

  @JsonCreator
  public SignalOutputsDefinition(
      @JsonProperty("definitions") List<MapParamDefinition> definitions) {
    this.definitions = definitions;
  }

  @Override
  public StepOutputType getType() {
    return StepOutputType.SIGNAL;
  }

  @Override
  public SignalOutputsDefinition asSignalOutputsDefinition() {
    return this;
  }
}
