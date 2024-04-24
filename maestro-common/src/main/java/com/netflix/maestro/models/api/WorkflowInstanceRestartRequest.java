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
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.validations.RunParamsConstraint;
import java.util.Map;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Run request to restart a workflow run for a specific workflow. request_id is used for
 * deduplication.
 */
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {"request_time", "request_id", "restart_policy", "run_params", "artifacts"},
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

  /** set runParams. */
  public void setRunParams(Map<String, ParamDefinition> input) {
    this.runParams = ParamDefinition.preprocessDefinitionParams(input);
  }
}
