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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.parameter.BooleanParameter;
import com.netflix.maestro.models.parameter.Parameter;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/** Step instance transition data model. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"predecessors", "successors"},
    alphabetic = true)
@Data
public class StepInstanceTransition {
  /** Derived from enriched step definition. If empty, this is the root node. */
  @NotNull private List<String> predecessors = new ArrayList<>();

  /**
   * This is required, where, the key is step id and its value is parameter defined using SEL
   * expression for condition based on the user defined step transition.
   */
  @NotNull private Map<String, Parameter> successors = new LinkedHashMap<>();

  /** Create step instance transition from step definition. */
  public static StepInstanceTransition from(Step stepDefinition) {
    StepInstanceTransition transition = new StepInstanceTransition();
    transition.setPredecessors(stepDefinition.getTransition().getPredecessors());
    Map<String, Parameter> nextStepConditions = new LinkedHashMap<>();
    stepDefinition
        .getTransition()
        .getSuccessors()
        .forEach(
            (successor, expr) ->
                nextStepConditions.put(
                    successor,
                    BooleanParameter.builder().name(successor).expression(expr).build()));
    transition.setSuccessors(nextStepConditions);
    return transition;
  }
}
