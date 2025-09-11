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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.validation.Valid;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Foreach Step definition with additional fields.
 *
 * <p>Note that there is no step retry for foreach step. It might fail due to various reasons, e.g.
 * invalid SEL expression evaluation, etc. But retries won't help in those cases.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "id",
      "name",
      "description",
      "transition",
      "failure_mode",
      "tags",
      "timeout",
      "condition",
      "signal_dependencies",
      "signal_outputs",
      "params",
      "steps"
    },
    alphabetic = true)
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class WhileStep extends AbstractStep {
  private String condition; // SEL expression to be evaluated before each iteration
  @Valid private List<Step> steps;

  @JsonIgnore
  @Override
  public StepType getType() {
    return StepType.WHILE;
  }
}
