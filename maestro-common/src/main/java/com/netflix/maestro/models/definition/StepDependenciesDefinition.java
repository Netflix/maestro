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
import com.netflix.maestro.utils.Checks;
import java.util.List;
import lombok.Data;

/** Request payload for step dependencies. */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"definitions", "type"},
    alphabetic = true)
@Data
public class StepDependenciesDefinition {
  /** param name for step dependency name. */
  public static final String STEP_DEPENDENCY_NAME = "name";

  /** param name for step dependency sub type, like input_table, input_s3. */
  public static final String STEP_DEPENDENCY_SUB_TYPE = "_step_dependency_sub_type";

  private final List<MapParamDefinition> definitions;
  private final StepDependencyType type;

  @JsonCreator
  public StepDependenciesDefinition(
      @JsonProperty("definitions") List<MapParamDefinition> definitions,
      @JsonProperty("type") StepDependencyType type) {
    Checks.checkTrue(type != null, "type cannot be null");
    this.definitions = definitions;
    this.type = type;
  }

  public static boolean isReservedParamName(String param) {
    return param.equals(STEP_DEPENDENCY_NAME) || param.equals(STEP_DEPENDENCY_SUB_TYPE);
  }
}
