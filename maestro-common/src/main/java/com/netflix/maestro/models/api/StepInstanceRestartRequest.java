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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.netflix.maestro.annotations.Nullable;
import com.netflix.maestro.models.instance.RunPolicy;
import com.netflix.maestro.models.parameter.ParamDefinition;
import com.netflix.maestro.validations.RunParamsConstraint;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

/**
 * Run request to start/restart a workflow run for a specific workflow. request_id is used for
 * deduplication.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(
    value = {
      "request_time",
      "request_id",
      "restart_policy",
      "step_run_params",
      "upstream_restart_mode"
    },
    alphabetic = true)
@Data
public class StepInstanceRestartRequest {
  // The time that the restart is requested. By default, it is request received time.
  private long requestTime = System.currentTimeMillis();

  private UUID requestId;

  // restart policy for its downstream
  @NotNull private RestartPolicy restartPolicy;

  // for restart, the validation should make sure some params cannot be mutated
  @Valid @RunParamsConstraint
  private Map<String, ParamDefinition> stepRunParams; // step runtime parameter overrides

  // upstream restart mode defines whether to restart upstream in case of step restart
  @Nullable private UpstreamRestartMode upstreamRestartMode;

  /** set runParams. */
  public void setStepRunParams(Map<String, ParamDefinition> input) {
    this.stepRunParams = ParamDefinition.preprocessDefinitionParams(input);
  }

  @JsonIgnore
  public RunPolicy getRestartRunPolicyWithUpstreamRestartMode() {
    if (upstreamRestartMode == UpstreamRestartMode.RESTART_FROM_INLINE_ROOT) {
      return RunPolicy.valueOf(restartPolicy.name());
    } else {
      return RunPolicy.RESTART_FROM_SPECIFIC;
    }
  }
}
