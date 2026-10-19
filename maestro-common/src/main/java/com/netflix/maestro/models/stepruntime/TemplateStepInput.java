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
package com.netflix.maestro.models.stepruntime;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.parameter.ParamType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * A position in a job template where a workflow may attach steps of its own. The template owner
 * declares where the position sits ({@code predecessors} and {@code successors} are template step
 * ids), which template params the attached steps may reference ({@code params}), and what every
 * attached step must publish ({@code outputs}). The position name is the key the workflow uses in
 * its {@code step_inputs} and, when outputs are declared, the id of the join step the template's
 * own steps read the collected outputs from, as {@code <position>__<output>}.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"predecessors", "successors", "params", "outputs"},
    alphabetic = true)
@Data
public class TemplateStepInput {
  /** Template step ids that run before the attached steps. */
  private List<String> predecessors;

  /** Template step ids that run after the attached steps. */
  private List<String> successors;

  /** Template param names the attached steps may reference. */
  private List<String> params;

  /** Params every attached step must publish, collected on the join step as arrays. */
  @Valid private Map<String, Output> outputs;

  /** True when the position collects outputs from its attached steps. */
  @JsonIgnore
  public boolean hasOutputs() {
    return outputs != null && !outputs.isEmpty();
  }

  /** Declaration of one output every attached step must publish. */
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Data
  public static class Output {
    @NotNull private ParamType type;
  }
}
