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
package com.netflix.maestro.models.instance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.definition.StepOutputsDefinition;
import com.netflix.maestro.models.parameter.MapParameter;
import com.netflix.maestro.models.timeline.TimelineLogEvent;
import java.util.List;
import lombok.Data;

/** Signal output. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonPropertyOrder(
    value = {"type", "outputs", "info"},
    alphabetic = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class SignalStepOutputs implements StepOutputs {
  private final List<SignalStepOutput> outputs;

  @Nullable private TimelineLogEvent info;

  @JsonCreator
  public SignalStepOutputs(@JsonProperty("outputs") List<SignalStepOutput> outputs) {
    this.outputs = outputs;
  }

  @Override
  public StepOutputsDefinition.StepOutputType getType() {
    return StepOutputsDefinition.StepOutputType.SIGNAL;
  }

  @Override
  public SignalStepOutputs asSignalStepOutputs() {
    return this;
  }

  @JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
  @JsonPropertyOrder(
      value = {"param", "output_signal_instance"},
      alphabetic = true)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Data
  public static class SignalStepOutput {
    private static final String NAME_PARAM = "name";
    private final MapParameter param;
    private OutputSignalInstance outputSignalInstance;

    @JsonCreator
    public SignalStepOutput(
        @JsonProperty("param") MapParameter param,
        @JsonProperty("output_signal_instance") OutputSignalInstance outputSignalInstance) {
      this.param = param;
      this.outputSignalInstance = outputSignalInstance;
    }

    @JsonIgnore
    public String getName() {
      return param.getEvaluatedParam(NAME_PARAM).asString();
    }
  }
}
