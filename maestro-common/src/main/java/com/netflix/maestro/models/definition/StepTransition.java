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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.utils.Checks;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/** Step transition data model class. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder(
    value = {"predecessors", "successors"},
    alphabetic = true)
@Data
public class StepTransition {
  /**
   * This is optional. Callers do not need to provide it. If empty, will derive it from DAG during
   * workflow instance creation at start time. If not empty, will validate it during workflow
   * creation at PUT endpoint.
   */
  private List<String> predecessors = new ArrayList<>();

  /**
   * This is required, where, the key is step id and its value is SEL expression string for
   * condition.
   */
  private Map<String, String> successors = new LinkedHashMap<>();

  /** set predecessors. */
  public void setPredecessors(List<String> stepIds) {
    Checks.checkTrue(
        new HashSet<>(stepIds).size() == stepIds.size(),
        "Invalid successor list as there are duplicate step ids: %s",
        stepIds);
    predecessors = stepIds;
  }
}
