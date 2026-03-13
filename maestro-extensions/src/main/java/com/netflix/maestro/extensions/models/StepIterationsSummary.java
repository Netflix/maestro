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
package com.netflix.maestro.extensions.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.instance.StepInstance;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.ToString;

/** Represents stepIterationsStats data model in the context of foreach flattening. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"representative_iteration", "count_by_status", "range", "loop_param_values"},
    alphabetic = true)
@Data
@ToString
public class StepIterationsSummary {
  private StepIteration representativeIteration;
  private Map<StepInstance.Status, Long> countByStatus;
  private Long[] range;
  private Map<String, List<String>> loopParamValues;
}
