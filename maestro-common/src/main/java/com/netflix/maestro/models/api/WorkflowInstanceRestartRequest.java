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
package com.netflix.maestro.models.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.instance.StepSelection;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.validations.RunParamsConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

/**
 * Run request to restart a workflow run for a specific workflow. request_id is used for
 * deduplication.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "request_time",
      "request_id",
      "restart_policy",
      "run_params",
      "step_selection",
      "artifacts"
    },
    alphabetic = true)
@Data
public class WorkflowInstanceRestartRequest {
  // The time that the restart is requested. By default, it is request received time.
  private long requestTime = System.currentTimeMillis();

  private UUID requestId;

  // restart policy for itself and its downstream
  @NotNull private RestartPolicy restartPolicy;

  // for restart, the validation should make sure some params cannot be mutated
  @Valid @RunParamsConstraint
  private Map<String, ParamDefinition> runParams; // runtime parameter overrides

  /**
   * Step selection for this new run.
   *
   * <ul>
   *   <li>{@code null}: inherit the baseline run's selection, so the steps it skipped stay skipped.
   *   <li>Non-empty: use this selection instead of the baseline run's.
   *   <li>Empty: discard the inherited selection and run every step.
   * </ul>
   */
  @Valid private StepSelection stepSelection;

  /** set runParams. */
  public void setRunParams(Map<String, ParamDefinition> input) {
    this.runParams = ParamDefinition.preprocessDefinitionParams(input);
  }
}
