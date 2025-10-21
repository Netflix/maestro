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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.validations.RetryPolicyConstraint;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Step definition for all step types with additional fields. */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "id",
      "name",
      "description",
      "transition",
      "type",
      "sub_type",
      "sub_type_version",
      "failure_mode",
      "retry_policy",
      "tags",
      "timeout",
      "signal_dependencies",
      "signal_outputs",
      "params"
    },
    alphabetic = true)
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public final class TypedStep extends AbstractStep {
  @Getter(onMethod = @__({@Override}))
  @NotNull
  private StepType type;

  @Getter(onMethod = @__({@Override}))
  private String subType; // optional

  private String subTypeVersion; // optional

  @JsonProperty
  @Getter(onMethod = @__({@Override}))
  @RetryPolicyConstraint
  private RetryPolicy retryPolicy;
}
