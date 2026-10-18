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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.definition.Step;
import com.netflix.maestro.models.parameter.ParamDefinition;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * The payload every template preparer produces: the step list the template step runs as an inline
 * workflow instance and additional run params for that instance. It is never a full workflow
 * definition.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"steps", "params"},
    alphabetic = true)
@Data
public class TemplateExpansion {
  @Valid private List<Step> steps;

  @Valid private Map<String, ParamDefinition> params;

  /** set params. */
  public void setParams(Map<String, ParamDefinition> input) {
    this.params = ParamDefinition.preprocessDefinitionParams(input);
  }
}
